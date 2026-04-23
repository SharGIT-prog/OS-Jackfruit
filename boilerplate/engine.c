/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"


#include <sys/resource.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* ---------------------------------------------------------------
 * cgroups cpu.weight table: maps nice value [-20..19] to a cgroup
 * --------------------------------------------------------------- */
static const int nice_to_cgroup_weight[40] = {
    1872, 1616, 1396, 1206, 1041,  900,  777,  671,
     579,  500,  432,  373,  322,  278,  240,  208,
     179,  155,  134,  115,  100,   86,   74,   64,
      55,   48,   41,   35,   30,   26,   23,   19,
      17,   14,   12,   20,    9,    8,    7,    6,
};

static void apply_cpu_cgroup(const char *container_id, int nice_val, pid_t pid)
{
    char dir[256], path[300];
    int  idx    = nice_val + 20;
    int  weight = (idx >= 0 && idx < 40) ? nice_to_cgroup_weight[idx] : 100;

    /* Try cgroups v2 first (Ubuntu 22.04/24.04 default) */
    snprintf(dir, sizeof(dir),
             "/sys/fs/cgroup/engine_containers/%s", container_id);
    if (mkdir("/sys/fs/cgroup/engine_containers", 0755) == 0 ||
        errno == EEXIST) {
        if (mkdir(dir, 0755) == 0 || errno == EEXIST) {
            /* cpu.weight: cgroups v2 */
            snprintf(path, sizeof(path), "%s/cpu.weight", dir);
            FILE *f = fopen(path, "w");
            if (f) {
                fprintf(f, "%d\n", weight);
                fclose(f);
                /* add pid to cgroup */
                snprintf(path, sizeof(path), "%s/cgroup.procs", dir);
                f = fopen(path, "w");
                if (f) { fprintf(f, "%d\n", (int)pid); fclose(f); return; }
            }
        }
    }

    /* Fall back to cgroups v1 cpu.shares */
    snprintf(dir, sizeof(dir),
             "/sys/fs/cgroup/cpu/engine_containers/%s", container_id);
    mkdir("/sys/fs/cgroup/cpu/engine_containers", 0755);
    if (mkdir(dir, 0755) == 0 || errno == EEXIST) {
        int shares = weight * 1024 / 100;  /* scale to v1 range */
        snprintf(path, sizeof(path), "%s/cpu.shares", dir);
        FILE *f = fopen(path, "w");
        if (f) {
            fprintf(f, "%d\n", shares);
            fclose(f);
            snprintf(path, sizeof(path), "%s/tasks", dir);
            f = fopen(path, "w");
            if (f) { fprintf(f, "%d\n", (int)pid); fclose(f); }
        }
    }
    (void)container_id;
}

static void remove_cpu_cgroup(const char *container_id)
{
    char dir[256];
    /* Try v2 first */
    snprintf(dir, sizeof(dir),
             "/sys/fs/cgroup/engine_containers/%s", container_id);
    rmdir(dir);
    /* Try v1 */
    snprintf(dir, sizeof(dir),
             "/sys/fs/cgroup/cpu/engine_containers/%s", container_id);
    rmdir(dir);
}



/* ---------------------------------------------------------------
 * Constants
 * --------------------------------------------------------------- */
#define STACK_SIZE           (1024 * 1024)
#define CONTAINER_ID_LEN     32
#define CONTROL_PATH         "/tmp/mini_runtime.sock"
#define LOG_DIR              "logs"
#define CONTROL_MESSAGE_LEN  512
#define CHILD_COMMAND_LEN    512
#define LOG_CHUNK_SIZE       4096
#define LOG_BUFFER_CAPACITY  32
#define DEFAULT_SOFT_LIMIT   (40UL << 20)
#define DEFAULT_HARD_LIMIT   (64UL << 20)
#define MONITOR_DEV          "/dev/container_monitor"
#define MAX_CONTAINERS       64

#define PROTO_OK             0
#define PROTO_ERR            1
#define PROTO_RUN_WAIT       2

/* ---------------------------------------------------------------
 * Enumerations
 * --------------------------------------------------------------- */
typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING          = 0,
    CONTAINER_RUNNING           = 1,
    CONTAINER_STOPPED           = 2,
    CONTAINER_KILLED            = 3,
    CONTAINER_EXITED            = 4,
    CONTAINER_HARD_LIMIT_KILLED = 5
} container_state_t;

/* ---------------------------------------------------------------
 * Structs
 * --------------------------------------------------------------- */
typedef struct container_record {
    char               id[CONTAINER_ID_LEN];
    pid_t              host_pid;
    time_t             started_at;
    container_state_t  state;
    unsigned long      soft_limit_bytes;
    unsigned long      hard_limit_bytes;
    int                exit_code;
    int                exit_signal;
    int                stop_requested;
    char               log_path[PATH_MAX];
    char               rootfs[PATH_MAX];
    pthread_t          producer_tid;
    int                producer_running;
    struct container_record *next;
} container_record_t;

typedef struct {
    char   container_id[CONTAINER_ID_LEN];
    size_t length;
    char   data[LOG_CHUNK_SIZE];
    int    is_eof;
} log_item_t;

typedef struct {
    log_item_t      items[LOG_BUFFER_CAPACITY];
    size_t          head;
    size_t          tail;
    size_t          count;
    int             shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char           container_id[CONTAINER_ID_LEN];
    char           rootfs[PATH_MAX];
    char           argv_blob[CHILD_COMMAND_LEN];
    int            argv_argc;
    unsigned long  soft_limit_bytes;
    unsigned long  hard_limit_bytes;
    int            nice_value;
} control_request_t;

typedef struct {
    int  status;
    int  exit_code;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

/*
 * child_config_t — passed to clone child.
 * IMPORTANT: all fields are value-copied into local variables on the clone
 * stack at the very start of child_fn() BEFORE the ready byte is written.
 * This is what makes free(cfg) safe in the parent.
 */
typedef struct {
    char rootfs[PATH_MAX];
    char container_id[CONTAINER_ID_LEN];
    char argv_blob[CHILD_COMMAND_LEN];
    int  argv_argc;
    int  nice_value;
    int  pipe_write_fd;   /* container stdout/stderr → supervisor */
    int  ready_write_fd;  /* child writes 1 byte here when done with cfg */
} child_config_t;

typedef struct {
    int               pipe_read_fd;
    char              container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buf;
} producer_arg_t;

typedef struct {
    int                server_fd;
    int                monitor_fd;
    volatile int       should_stop;
    pthread_t          logger_tid;
    bounded_buffer_t   log_buffer;
    pthread_mutex_t    metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static supervisor_ctx_t *g_ctx                          = NULL;
static volatile int      g_run_stop_requested           = 0;
static char              g_run_container_id[CONTAINER_ID_LEN];

/* ---------------------------------------------------------------
 * argv packing / unpacking
 * --------------------------------------------------------------- */
static void pack_argv(control_request_t *req, int argc, char *argv[])
{
    int    i;
    size_t off = 0;
    req->argv_argc = argc;
    memset(req->argv_blob, 0, sizeof(req->argv_blob));
    for (i = 0; i < argc; i++) {
        size_t len = strlen(argv[i]);
        if (off + len + 1 >= sizeof(req->argv_blob)) break;
        memcpy(req->argv_blob + off, argv[i], len + 1);
        off += len + 1;
    }
}

static void unpack_argv(const char *src_blob, int argc,
                        char *ptrs[], int max_ptrs,
                        char *blob, size_t blob_size)
{
    int    i   = 0;
    size_t off = 0;
    memcpy(blob, src_blob, blob_size);
    while (i < argc && i < max_ptrs - 1 && off < blob_size) {
        ptrs[i++] = blob + off;
        off       += strlen(blob + off) + 1;
    }
    ptrs[i] = NULL;
}

/* ---------------------------------------------------------------
 * Utility
 * --------------------------------------------------------------- */
static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage:\n"
        "  %s supervisor <base-rootfs>\n"
        "  %s start <id> <rootfs> <cmd...> [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s run   <id> <rootfs> <cmd...> [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s ps\n"
        "  %s logs <id>\n"
        "  %s stop <id>\n",
        prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag, const char *value,
                           unsigned long *out)
{
    char *end = NULL;
    unsigned long mib;
    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s too large\n", flag);
        return -1;
    }
    *out = mib * (1UL << 20);
    return 0;
}

static const char *state_to_string(container_state_t s)
{
    switch (s) {
    case CONTAINER_STARTING:          return "starting";
    case CONTAINER_RUNNING:           return "running";
    case CONTAINER_STOPPED:           return "stopped";
    case CONTAINER_KILLED:            return "killed";
    case CONTAINER_EXITED:            return "exited";
    case CONTAINER_HARD_LIMIT_KILLED: return "hard_limit_killed";
    default:                          return "unknown";
    }
}

static int validate_rootfs(const char *path)
{
    struct stat st;
    if (stat(path, &st) != 0) {
        fprintf(stderr, "rootfs path does not exist: %s (%s)\n",
                path, strerror(errno));
        return -1;
    }
    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "rootfs path is not a directory: %s\n", path);
        return -1;
    }
    return 0;
}

/* ---------------------------------------------------------------
 * CLI flag + command splitter
 * --------------------------------------------------------------- */
static int split_flags_and_command(control_request_t *req,
                                    int argc, char *argv[], int start,
                                    char **cmd_out[], int *cmd_argc_out)
{
    static char *cmd_argv[64];
    int cmd_argc = 0;
    int i = start;

    while (i < argc) {
        if ((strcmp(argv[i], "--soft-mib") == 0 ||
             strcmp(argv[i], "--hard-mib") == 0 ||
             strcmp(argv[i], "--nice")     == 0) && i + 1 < argc) {

            if (strcmp(argv[i], "--soft-mib") == 0) {
                if (parse_mib_flag("--soft-mib", argv[i+1],
                                    &req->soft_limit_bytes) != 0) return -1;
            } else if (strcmp(argv[i], "--hard-mib") == 0) {
                if (parse_mib_flag("--hard-mib", argv[i+1],
                                    &req->hard_limit_bytes) != 0) return -1;
            } else {
                char *end = NULL;
                long nv;
                errno = 0;
                nv = strtol(argv[i+1], &end, 10);
                if (errno || end == argv[i+1] || *end || nv < -20 || nv > 19) {
                    fprintf(stderr,
                        "Invalid --nice value (must be -20..19): %s\n",
                        argv[i+1]);
                    return -1;
                }
                req->nice_value = (int)nv;
            }
            i += 2;
        } else {
            if (cmd_argc < 63) cmd_argv[cmd_argc++] = argv[i];
            i++;
        }
    }
    cmd_argv[cmd_argc] = NULL;
    *cmd_out      = cmd_argv;
    *cmd_argc_out = cmd_argc;

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

/* ---------------------------------------------------------------
 * Bounded buffer
 * --------------------------------------------------------------- */
static int bounded_buffer_init(bounded_buffer_t *buf)
{
    int rc;
    memset(buf, 0, sizeof(*buf));
    rc = pthread_mutex_init(&buf->mutex, NULL);
    if (rc) return rc;
    rc = pthread_cond_init(&buf->not_empty, NULL);
    if (rc) { pthread_mutex_destroy(&buf->mutex); return rc; }
    rc = pthread_cond_init(&buf->not_full, NULL);
    if (rc) {
        pthread_cond_destroy(&buf->not_empty);
        pthread_mutex_destroy(&buf->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buf)
{
    pthread_cond_destroy(&buf->not_full);
    pthread_cond_destroy(&buf->not_empty);
    pthread_mutex_destroy(&buf->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buf)
{
    pthread_mutex_lock(&buf->mutex);
    buf->shutting_down = 1;
    pthread_cond_broadcast(&buf->not_empty);
    pthread_cond_broadcast(&buf->not_full);
    pthread_mutex_unlock(&buf->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buf, const log_item_t *item)
{
    pthread_mutex_lock(&buf->mutex);
    while (buf->count == LOG_BUFFER_CAPACITY && !buf->shutting_down)
        pthread_cond_wait(&buf->not_full, &buf->mutex);

    if (buf->shutting_down && buf->count == LOG_BUFFER_CAPACITY) {
        pthread_mutex_unlock(&buf->mutex);
        return -1;
    }
    buf->items[buf->tail] = *item;
    buf->tail = (buf->tail + 1) % LOG_BUFFER_CAPACITY;
    buf->count++;
    pthread_cond_signal(&buf->not_empty);
    pthread_mutex_unlock(&buf->mutex);
    return 0;
}

static int bounded_buffer_pop(bounded_buffer_t *buf, log_item_t *item)
{
    pthread_mutex_lock(&buf->mutex);
    while (buf->count == 0) {
        if (buf->shutting_down) {
            pthread_mutex_unlock(&buf->mutex);
            return 1;
        }
        pthread_cond_wait(&buf->not_empty, &buf->mutex);
    }
    *item = buf->items[buf->head];
    buf->head = (buf->head + 1) % LOG_BUFFER_CAPACITY;
    buf->count--;
    pthread_cond_signal(&buf->not_full);
    pthread_mutex_unlock(&buf->mutex);
    return 0;
}

/* ---------------------------------------------------------------
 * Logger consumer thread
 * --------------------------------------------------------------- */
static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    bounded_buffer_t *buf = &ctx->log_buffer;

    fprintf(stderr, "[logger] consumer thread started\n");

    for (;;) {
        log_item_t item;
        int rc = bounded_buffer_pop(buf, &item);
        if (rc != 0) break;

        if (item.is_eof) {
            fprintf(stderr, "[logger] EOF for container=%s\n",
                    item.container_id);
            continue;
        }

        char log_path[PATH_MAX];
        log_path[0] = '\0';
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c) {
            if (strcmp(c->id, item.container_id) == 0) {
                strncpy(log_path, c->log_path, sizeof(log_path) - 1);
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path[0] == '\0') continue;

        int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            fprintf(stderr, "[logger] cannot open %s: %s\n",
                    log_path, strerror(errno));
            continue;
        }
        size_t done = 0;
        while (done < item.length) {
            ssize_t n = write(fd, item.data + done, item.length - done);
            if (n <= 0) break;
            done += (size_t)n;
        }
        close(fd);
    }

    fprintf(stderr, "[logger] consumer thread exiting\n");
    return NULL;
}

/* ---------------------------------------------------------------
 * Producer thread — JOINABLE
 * --------------------------------------------------------------- */
static void *producer_thread(void *arg)
{
    producer_arg_t   *parg = (producer_arg_t *)arg;
    int               fd   = parg->pipe_read_fd;
    bounded_buffer_t *buf  = parg->buf;
    char cid[CONTAINER_ID_LEN];
    strncpy(cid, parg->container_id, sizeof(cid) - 1);
    free(parg);

    fprintf(stderr, "[producer:%s] thread started\n", cid);

    for (;;) {
        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, cid, sizeof(item.container_id) - 1);

        ssize_t n = read(fd, item.data, LOG_CHUNK_SIZE);
        if (n <= 0) break;
        item.length = (size_t)n;
        item.is_eof = 0;
        bounded_buffer_push(buf, &item);
    }
    close(fd);

    log_item_t eof;
    memset(&eof, 0, sizeof(eof));
    strncpy(eof.container_id, cid, sizeof(eof.container_id) - 1);
    eof.is_eof = 1;
    bounded_buffer_push(buf, &eof);

    fprintf(stderr, "[producer:%s] thread exiting\n", cid);
    return NULL;
}

/* ---------------------------------------------------------------
 * Clone child entrypoint
 *
 * CRITICAL: copy all fields from cfg into LOCAL VARIABLES first, then
 * write the ready byte so the parent knows it is safe to free cfg.
 * Everything after the ready_write is independent of cfg.
 * --------------------------------------------------------------- */
static int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* ---- Step 1: copy all cfg fields into LOCAL (stack) variables ---- */
    char  local_rootfs[PATH_MAX];
    char  local_id[CONTAINER_ID_LEN];
    char  local_blob[CHILD_COMMAND_LEN];
    int   local_argc     = cfg->argv_argc;
    int   local_nice     = cfg->nice_value;
    int   local_pipe_wfd = cfg->pipe_write_fd;
    int   local_ready_wfd = cfg->ready_write_fd;

    strncpy(local_rootfs, cfg->rootfs,       sizeof(local_rootfs) - 1);
    local_rootfs[sizeof(local_rootfs)-1] = '\0';
    strncpy(local_id,     cfg->container_id, sizeof(local_id) - 1);
    local_id[sizeof(local_id)-1] = '\0';
    memcpy(local_blob, cfg->argv_blob, sizeof(local_blob));

    /* ---- Step 2: signal parent that cfg is no longer needed ---- */
    {
        char ready = 1;
        write(local_ready_wfd, &ready, 1);
        close(local_ready_wfd);
    }

    /* ---- Step 3: redirect stdout/stderr to logging pipe ---- */
    if (local_pipe_wfd >= 0) {
        dup2(local_pipe_wfd, STDOUT_FILENO);
        dup2(local_pipe_wfd, STDERR_FILENO);
        close(local_pipe_wfd);
    }

    /* ---- Step 4: set hostname (UTS namespace) ---- */
    sethostname(local_id, strlen(local_id));

    /* ---- Step 5: chroot into container rootfs ---- */
    if (chroot(local_rootfs) != 0) {
        /* stderr already goes to pipe — parent will see this in logs */
        write(STDERR_FILENO, "child: chroot failed\n", 21);
        return 1;
    }
    if (chdir("/") != 0) {
        write(STDERR_FILENO, "child: chdir / failed\n", 22);
        return 1;
    }

    /* ---- Step 6: mount /proc ---- */
    mount("proc", "/proc", "proc", 0, NULL);

    /* ---- Step 7: apply nice value (absolute) and pin to CPU 0 ---- */
        if (local_nice != 0) {
            setpriority(PRIO_PROCESS, 0, local_nice);  /* absolute, not relative */
    }

    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(0, &cpuset);
        sched_setaffinity(0, sizeof(cpuset), &cpuset);
    }

    /* ---- Step 8: unpack and exec ---- */
    char  blob_copy[CHILD_COMMAND_LEN];
    char *ptrs[64];
    unpack_argv(local_blob, local_argc, ptrs, 64, blob_copy, sizeof(blob_copy));

    if (ptrs[0] == NULL) {
        write(STDERR_FILENO, "child: no command\n", 18);
        return 1;
    }

    execv(ptrs[0], ptrs);

    /* execv failed — write error to pipe so it appears in container logs */
    {
        char errmsg[128];
        int len = snprintf(errmsg, sizeof(errmsg),
                           "child: execv(%s) failed: %s\n",
                           ptrs[0], strerror(errno));
        write(STDERR_FILENO, errmsg, (size_t)len);
    }
    return 1;
}

/* ---------------------------------------------------------------
 * Kernel monitor helpers
 * --------------------------------------------------------------- */
static int register_with_monitor(int mfd, const char *cid, pid_t pid,
                                   unsigned long soft, unsigned long hard)
{
    if (mfd < 0) return 0;
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid              = pid;
    req.soft_limit_bytes = soft;
    req.hard_limit_bytes = hard;
    strncpy(req.container_id, cid, sizeof(req.container_id) - 1);
    if (ioctl(mfd, MONITOR_REGISTER, &req) < 0) {
        perror("ioctl MONITOR_REGISTER");
        return -1;
    }
    return 0;
}

static void unregister_from_monitor(int mfd, const char *cid, pid_t pid)
{
    if (mfd < 0) return;
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = pid;
    strncpy(req.container_id, cid, sizeof(req.container_id) - 1);
    ioctl(mfd, MONITOR_UNREGISTER, &req);
}

/* ---------------------------------------------------------------
 * Metadata helpers  (caller must hold metadata_lock)
 * --------------------------------------------------------------- */
static container_record_t *find_container(supervisor_ctx_t *ctx,
                                           const char *id)
{
    container_record_t *c = ctx->containers;
    while (c) {
        if (strcmp(c->id, id) == 0) return c;
        c = c->next;
    }
    return NULL;
}

static int rootfs_in_use(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *c = ctx->containers;
    while (c) {
        if ((c->state == CONTAINER_RUNNING ||
             c->state == CONTAINER_STARTING) &&
            strcmp(c->rootfs, rootfs) == 0)
            return 1;
        c = c->next;
    }
    return 0;
}

static int count_live(supervisor_ctx_t *ctx)
{
    int n = 0;
    container_record_t *c = ctx->containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING ||
            c->state == CONTAINER_STARTING) n++;
        c = c->next;
    }
    return n;
}

static void add_container(supervisor_ctx_t *ctx, container_record_t *rec)
{
    rec->next = ctx->containers;
    ctx->containers = rec;
}

/* ---------------------------------------------------------------
 * Launch a container
 *
 * Uses a "ready pipe" so the parent waits until the child has copied
 * all fields out of cfg before freeing it.
 * --------------------------------------------------------------- */
static int launch_container(supervisor_ctx_t *ctx,
                              container_record_t *rec,
                              const control_request_t *creq)
{
    /* Logging pipe: child writes, supervisor's producer thread reads */
    int logpipe[2];
    if (pipe2(logpipe, O_CLOEXEC) != 0) { perror("pipe2 logpipe"); return -1; }

    /* Ready pipe: child writes 1 byte when done with cfg; parent reads it */
    int readypipe[2];
    if (pipe2(readypipe, O_CLOEXEC) != 0) {
        perror("pipe2 readypipe");
        close(logpipe[0]); close(logpipe[1]);
        return -1;
    }

    mkdir(LOG_DIR, 0755);
    snprintf(rec->log_path, sizeof(rec->log_path),
             "%s/%s.log", LOG_DIR, rec->id);

    child_config_t *cfg = calloc(1, sizeof(*cfg));
    if (!cfg) {
        close(logpipe[0]);  close(logpipe[1]);
        close(readypipe[0]); close(readypipe[1]);
        return -1;
    }

    strncpy(cfg->rootfs,       creq->rootfs,      sizeof(cfg->rootfs) - 1);
    strncpy(cfg->container_id, rec->id,            sizeof(cfg->container_id) - 1);
    memcpy(cfg->argv_blob, creq->argv_blob, sizeof(cfg->argv_blob));
    cfg->argv_argc      = creq->argv_argc;
    cfg->nice_value     = creq->nice_value;
    cfg->pipe_write_fd  = logpipe[1];
    cfg->ready_write_fd = readypipe[1];

    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        free(cfg);
        close(logpipe[0]);  close(logpipe[1]);
        close(readypipe[0]); close(readypipe[1]);
        return -1;
    }

    int clone_flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t pid = clone(child_fn, stack + STACK_SIZE, clone_flags, cfg);

    if (pid < 0) {
        perror("clone");
        free(stack); free(cfg);
        close(logpipe[0]);  close(logpipe[1]);
        close(readypipe[0]); close(readypipe[1]);
        return -1;
    }

    /*
     * Close the write ends in the parent:
     *   - logpipe[1]:   child (or the exec'd binary) holds the write end via fd=1
     *   - readypipe[1]: child holds it; we close parent's copy now
     */
    close(logpipe[1]);
    close(readypipe[1]);

    /*
     * WAIT for the child to signal it has copied cfg into local variables.
     * This eliminates the free(cfg) race entirely.
     */
    {
        char ready = 0;
        ssize_t n = read(readypipe[0], &ready, 1);
        (void)n; /* we don't care about the value, just that it arrived */
    }
    close(readypipe[0]);

    /* Now it is safe to free cfg and stack */
    free(stack);
    free(cfg);

    rec->host_pid   = pid;
    rec->started_at = time(NULL);
    rec->state      = CONTAINER_RUNNING;

    register_with_monitor(ctx->monitor_fd, rec->id, pid,
                          rec->soft_limit_bytes, rec->hard_limit_bytes);


    apply_cpu_cgroup(rec->id, creq->nice_value, pid);

    /* Spawn JOINABLE producer thread */
    producer_arg_t *parg = malloc(sizeof(*parg));
    if (!parg) {
        close(logpipe[0]);
        rec->producer_running = 0;
        return 0;
    }
    parg->pipe_read_fd = logpipe[0];
    strncpy(parg->container_id, rec->id, sizeof(parg->container_id) - 1);
    parg->buf = &ctx->log_buffer;

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    int rc = pthread_create(&rec->producer_tid, &attr, producer_thread, parg);
    pthread_attr_destroy(&attr);
    if (rc != 0) {
        perror("pthread_create producer");
        free(parg); close(logpipe[0]);
        rec->producer_running = 0;
    } else {
        rec->producer_running = 1;
    }
    return 0;
}

/* ---------------------------------------------------------------
 * Signal handlers
 * --------------------------------------------------------------- */
static void sigchld_handler(int sig)
{
    (void)sig;
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (!g_ctx) continue;
        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *c = g_ctx->containers;
        while (c) {
            if (c->host_pid == pid) {
                if (WIFEXITED(status)) {
                    c->exit_code   = WEXITSTATUS(status);
                    c->exit_signal = 0;
                    c->state       = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    c->exit_signal = WTERMSIG(status);
                    c->exit_code   = 128 + c->exit_signal;
                    if (c->stop_requested)
                        c->state = CONTAINER_STOPPED;
                    else if (c->exit_signal == SIGKILL)
                        c->state = CONTAINER_HARD_LIMIT_KILLED;
                    else
                        c->state = CONTAINER_KILLED;
                }
                unregister_from_monitor(g_ctx->monitor_fd, c->id, pid);
                remove_cpu_cgroup(c->id);
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

static void sigterm_supervisor_handler(int sig)
{
    (void)sig;
    if (g_ctx) g_ctx->should_stop = 1;
}

/* ---------------------------------------------------------------
 * Join producer threads for exited containers
 * --------------------------------------------------------------- */
static void reap_producer_threads(supervisor_ctx_t *ctx)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = ctx->containers;
    while (c) {
        if (c->producer_running &&
            c->state != CONTAINER_RUNNING &&
            c->state != CONTAINER_STARTING) {
            pthread_t tid = c->producer_tid;
            c->producer_running = 0;
            pthread_mutex_unlock(&ctx->metadata_lock);
            pthread_join(tid, NULL);
            fprintf(stderr,
                    "[supervisor] joined producer for container=%s\n", c->id);
            pthread_mutex_lock(&ctx->metadata_lock);
        }
        c = c->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

/* ---------------------------------------------------------------
 * Handle one control request
 * --------------------------------------------------------------- */
static void handle_request(supervisor_ctx_t *ctx,
                             const control_request_t *req,
                             control_response_t *resp,
                             int conn_fd)
{
    memset(resp, 0, sizeof(*resp));
    resp->status = PROTO_OK;

    switch (req->kind) {

    case CMD_START: {
        if (validate_rootfs(req->rootfs) != 0) {
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "Invalid rootfs: %s", req->rootfs);
            break;
        }
        pthread_mutex_lock(&ctx->metadata_lock);
        if (find_container(ctx, req->container_id)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "Container '%s' already exists", req->container_id);
            break;
        }
        if (rootfs_in_use(ctx, req->rootfs)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "rootfs '%s' already used by a live container",
                     req->rootfs);
            break;
        }
        if (count_live(ctx) >= MAX_CONTAINERS) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "Max container limit (%d) reached", MAX_CONTAINERS);
            break;
        }
        container_record_t *rec = calloc(1, sizeof(*rec));
        if (!rec) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = PROTO_ERR;
            strncpy(resp->message, "Out of memory", sizeof(resp->message)-1);
            break;
        }
        strncpy(rec->id,     req->container_id, sizeof(rec->id)     - 1);
        strncpy(rec->rootfs, req->rootfs,       sizeof(rec->rootfs) - 1);
        rec->soft_limit_bytes = req->soft_limit_bytes;
        rec->hard_limit_bytes = req->hard_limit_bytes;
        rec->state = CONTAINER_STARTING;
        add_container(ctx, rec);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (launch_container(ctx, rec, req) != 0) {
            pthread_mutex_lock(&ctx->metadata_lock);
            rec->state = CONTAINER_EXITED;
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "Failed to launch '%s'", req->container_id);
        } else {
            snprintf(resp->message, sizeof(resp->message),
                     "Container '%s' started (pid=%d)",
                     req->container_id, rec->host_pid);
        }
        break;
    }

    case CMD_RUN: {
        if (validate_rootfs(req->rootfs) != 0) {
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "Invalid rootfs: %s", req->rootfs);
            break;
        }
        pthread_mutex_lock(&ctx->metadata_lock);
        if (find_container(ctx, req->container_id)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "Container '%s' already exists", req->container_id);
            break;
        }
        if (rootfs_in_use(ctx, req->rootfs)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "rootfs '%s' already in use", req->rootfs);
            break;
        }
        if (count_live(ctx) >= MAX_CONTAINERS) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = PROTO_ERR;
            strncpy(resp->message, "Max container limit reached",
                    sizeof(resp->message)-1);
            break;
        }
        container_record_t *rec = calloc(1, sizeof(*rec));
        if (!rec) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = PROTO_ERR;
            strncpy(resp->message, "Out of memory", sizeof(resp->message)-1);
            break;
        }
        strncpy(rec->id,     req->container_id, sizeof(rec->id)     - 1);
        strncpy(rec->rootfs, req->rootfs,       sizeof(rec->rootfs) - 1);
        rec->soft_limit_bytes = req->soft_limit_bytes;
        rec->hard_limit_bytes = req->hard_limit_bytes;
        rec->state = CONTAINER_STARTING;
        add_container(ctx, rec);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (launch_container(ctx, rec, req) != 0) {
            pthread_mutex_lock(&ctx->metadata_lock);
            rec->state = CONTAINER_EXITED;
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "Failed to launch '%s'", req->container_id);
            break;
        }

        resp->status = PROTO_RUN_WAIT;
        snprintf(resp->message, sizeof(resp->message),
                 "Container '%s' started (pid=%d), waiting...",
                 req->container_id, rec->host_pid);
        if (write(conn_fd, resp, sizeof(*resp)) < 0)
            perror("write RUN_WAIT");

        while (1) {
            pthread_mutex_lock(&ctx->metadata_lock);
            container_record_t *cur =
                find_container(ctx, req->container_id);
            int done = cur &&
                cur->state != CONTAINER_RUNNING &&
                cur->state != CONTAINER_STARTING;
            if (done) {
                resp->status    = PROTO_OK;
                resp->exit_code = cur->exit_code;
                snprintf(resp->message, sizeof(resp->message),
                         "Container '%s' done state=%s exit_code=%d",
                         req->container_id,
                         state_to_string(cur->state),
                         cur->exit_code);
                pthread_mutex_unlock(&ctx->metadata_lock);
                break;
            }
            pthread_mutex_unlock(&ctx->metadata_lock);
            usleep(100000);
        }
        break;
    }

    case CMD_PS: {
        char buf[4096];
        int  off = 0;
        off += snprintf(buf + off, sizeof(buf) - (size_t)off,
            "%-16s %-8s %-20s %-10s %-10s %-5s %s\n",
            "ID", "PID", "STATE",
            "SOFT(MiB)", "HARD(MiB)", "EXIT", "STARTED");
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c && off < (int)sizeof(buf) - 1) {
            char ts[32];
            struct tm *tm = localtime(&c->started_at);
            strftime(ts, sizeof(ts), "%H:%M:%S", tm);
            off += snprintf(buf + off, sizeof(buf) - (size_t)off,
                "%-16s %-8d %-20s %-10lu %-10lu %-5d %s\n",
                c->id, c->host_pid,
                state_to_string(c->state),
                c->soft_limit_bytes >> 20,
                c->hard_limit_bytes >> 20,
                c->exit_code, ts);
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        strncpy(resp->message, buf, sizeof(resp->message) - 1);
        break;
    }

    case CMD_LOGS: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = find_container(ctx, req->container_id);
        char log_path[PATH_MAX];
        log_path[0] = '\0';
        if (c) strncpy(log_path, c->log_path, sizeof(log_path) - 1);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path[0] == '\0') {
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "No such container: %s", req->container_id);
            break;
        }
        int lfd = open(log_path, O_RDONLY);
        if (lfd < 0) {
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "Cannot open log %s: %s", log_path, strerror(errno));
            break;
        }
        resp->status = PROTO_OK;
        snprintf(resp->message, sizeof(resp->message),
                 "LOG_START:%s", log_path);
        if (write(conn_fd, resp, sizeof(*resp)) < 0)
            perror("write LOGS header");

        char rbuf[4096];
        ssize_t nr;
        while ((nr = read(lfd, rbuf, sizeof(rbuf))) > 0)
            if (write(conn_fd, rbuf, (size_t)nr) < 0) break;
        close(lfd);

        memset(resp, 0, sizeof(*resp));
        resp->status = PROTO_OK;
        strncpy(resp->message, "LOG_END", sizeof(resp->message) - 1);
        break;
    }

    case CMD_STOP: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = find_container(ctx, req->container_id);
        if (!c) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "No such container: %s", req->container_id);
            break;
        }
        if (c->state != CONTAINER_RUNNING &&
            c->state != CONTAINER_STARTING) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = PROTO_ERR;
            snprintf(resp->message, sizeof(resp->message),
                     "Container '%s' not running (state=%s)",
                     req->container_id, state_to_string(c->state));
            break;
        }
        c->stop_requested = 1;
        pid_t pid = c->host_pid;
        pthread_mutex_unlock(&ctx->metadata_lock);

        kill(pid, SIGTERM);

        for (int i = 0; i < 30; i++) {
            usleep(100000);
            pthread_mutex_lock(&ctx->metadata_lock);
            container_record_t *cur =
                find_container(ctx, req->container_id);
            int done = !cur ||
                (cur->state != CONTAINER_RUNNING &&
                 cur->state != CONTAINER_STARTING);
            pthread_mutex_unlock(&ctx->metadata_lock);
            if (done) goto stop_done;
        }
        kill(pid, SIGKILL);
stop_done:
        snprintf(resp->message, sizeof(resp->message),
                 "Stop signal sent to '%s' (pid=%d)",
                 req->container_id, pid);
        break;
    }

    default:
        resp->status = PROTO_ERR;
        strncpy(resp->message, "Unknown command", sizeof(resp->message)-1);
        break;
    }
}

/* ---------------------------------------------------------------
 * Supervisor main
 * --------------------------------------------------------------- */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    if (pthread_mutex_init(&ctx.metadata_lock, NULL) != 0) {
        perror("pthread_mutex_init"); return 1;
    }
    if (bounded_buffer_init(&ctx.log_buffer) != 0) {
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock); return 1;
    }

    ctx.monitor_fd = open(MONITOR_DEV, O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr,
            "[supervisor] Warning: %s unavailable (%s);"
            " memory monitoring disabled\n",
            MONITOR_DEV, strerror(errno));
    else
        fprintf(stderr, "[supervisor] Kernel monitor: %s\n", MONITOR_DEV);

    mkdir(LOG_DIR, 0755);

    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("bind"); close(ctx.server_fd); return 1;
    }
    if (listen(ctx.server_fd, 32) != 0) {
        perror("listen"); close(ctx.server_fd); return 1;
    }
    chmod(CONTROL_PATH, 0666);

    fprintf(stderr, "[supervisor] Listening on %s (base-rootfs=%s)\n",
            CONTROL_PATH, rootfs);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);
    sa.sa_handler = sigterm_supervisor_handler;
    sa.sa_flags   = 0;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    if (pthread_create(&ctx.logger_tid, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create logger");
        close(ctx.server_fd); return 1;
    }

    fcntl(ctx.server_fd, F_SETFL,
          fcntl(ctx.server_fd, F_GETFL, 0) | O_NONBLOCK);

    while (!ctx.should_stop) {
        int conn = accept(ctx.server_fd, NULL, NULL);
        if (conn < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                reap_producer_threads(&ctx);
                usleep(50000);
                continue;
            }
            if (errno == EINTR) continue;
            perror("accept"); break;
        }

        control_request_t req;
        ssize_t nr = recv(conn, &req, sizeof(req), MSG_WAITALL);
        if (nr != (ssize_t)sizeof(req)) {
            fprintf(stderr,
                "[supervisor] Incomplete request (%zd bytes)\n", nr);
            close(conn); continue;
        }

        control_response_t resp;
        handle_request(&ctx, &req, &resp, conn);
        if (write(conn, &resp, sizeof(resp)) < 0)
            perror("write response");
        close(conn);
    }

    fprintf(stderr, "[supervisor] Shutting down...\n");

    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING ||
            c->state == CONTAINER_STARTING) {
            c->stop_requested = 1;
            kill(c->host_pid, SIGTERM);
        }
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    for (int i = 0; i < 30; i++) {
        usleep(100000);
        pthread_mutex_lock(&ctx.metadata_lock);
        int live = count_live(&ctx);
        pthread_mutex_unlock(&ctx.metadata_lock);
        if (live == 0) break;
    }

    pthread_mutex_lock(&ctx.metadata_lock);
    c = ctx.containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING ||
            c->state == CONTAINER_STARTING)
            kill(c->host_pid, SIGKILL);
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);
    usleep(200000);

    reap_producer_threads(&ctx);
    pthread_mutex_lock(&ctx.metadata_lock);
    c = ctx.containers;
    while (c) {
        if (c->producer_running) {
            pthread_t tid = c->producer_tid;
            c->producer_running = 0;
            pthread_mutex_unlock(&ctx.metadata_lock);
            pthread_join(tid, NULL);
            pthread_mutex_lock(&ctx.metadata_lock);
        }
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_tid, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    pthread_mutex_lock(&ctx.metadata_lock);
    c = ctx.containers;
    while (c) {
        container_record_t *nx = c->next;
        free(c); c = nx;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    close(ctx.server_fd);
    unlink(CONTROL_PATH);

    fprintf(stderr, "[supervisor] Clean exit.\n");
    return 0;
}

/* ---------------------------------------------------------------
 * CLI signal handler for run()
 * --------------------------------------------------------------- */
static void run_client_sighandler(int sig)
{
    (void)sig;
    g_run_stop_requested = 1;
}

static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("connect (is the supervisor running?)");
        close(fd); return 1;
    }
    if (write(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write request"); close(fd); return 1;
    }

    control_response_t resp;

    if (req->kind == CMD_LOGS) {
        ssize_t nr = recv(fd, &resp, sizeof(resp), MSG_WAITALL);
        if (nr != (ssize_t)sizeof(resp)) { close(fd); return 1; }
        if (resp.status != PROTO_OK) {
            fprintf(stderr, "Error: %s\n", resp.message);
            close(fd); return 1;
        }
        fprintf(stdout, "=== %s ===\n", resp.message);
        char buf[4096];
        ssize_t n;
        while ((n = read(fd, buf, sizeof(buf))) > 0) {
            if ((size_t)n == sizeof(resp)) {
                control_response_t *m = (control_response_t *)(void *)buf;
                if (strncmp(m->message, "LOG_END", 7) == 0) break;
            }
            fwrite(buf, 1, (size_t)n, stdout);
        }
        fflush(stdout);
        close(fd); return 0;
    }

    if (req->kind == CMD_RUN) {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = run_client_sighandler;
        sa.sa_flags   = 0;
        sigaction(SIGINT,  &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);

        ssize_t nr = recv(fd, &resp, sizeof(resp), MSG_WAITALL);
        if (nr != (ssize_t)sizeof(resp)) { close(fd); return 1; }
        if (resp.status == PROTO_ERR) {
            fprintf(stderr, "Error: %s\n", resp.message);
            close(fd); return 1;
        }
        fprintf(stderr, "%s\n", resp.message);

        while (1) {
            if (g_run_stop_requested) {
                g_run_stop_requested = 0;
                int sfd = socket(AF_UNIX, SOCK_STREAM, 0);
                if (sfd >= 0 &&
                    connect(sfd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
                    control_request_t stopreq;
                    memset(&stopreq, 0, sizeof(stopreq));
                    stopreq.kind = CMD_STOP;
                    strncpy(stopreq.container_id, req->container_id,
                            sizeof(stopreq.container_id) - 1);
                    if (write(sfd, &stopreq, sizeof(stopreq)) > 0) {
                        control_response_t sr;
                        recv(sfd, &sr, sizeof(sr), MSG_WAITALL);
                        fprintf(stderr, "[run] forwarded stop: %s\n",
                                sr.message);
                    }
                    close(sfd);
                }
            }

            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(fd, &rfds);
            struct timeval tv = {0, 50000};
            int sel = select(fd + 1, &rfds, NULL, NULL, &tv);
            if (sel > 0) {
                nr = recv(fd, &resp, sizeof(resp), MSG_WAITALL);
                if (nr == (ssize_t)sizeof(resp)) break;
            }
        }

        if (resp.status != PROTO_OK) {
            fprintf(stderr, "Error: %s\n", resp.message);
            close(fd);
            return resp.exit_code ? resp.exit_code : 1;
        }
        printf("%s\n", resp.message);
        close(fd);
        return resp.exit_code;
    }

    ssize_t nr = recv(fd, &resp, sizeof(resp), MSG_WAITALL);
    if (nr != (ssize_t)sizeof(resp)) {
        fprintf(stderr, "Incomplete response\n");
        close(fd); return 1;
    }
    if (resp.status != PROTO_OK) {
        fprintf(stderr, "Error: %s\n", resp.message);
        close(fd); return 1;
    }
    printf("%s\n", resp.message);
    close(fd);
    return 0;
}

/* ---------------------------------------------------------------
 * CLI entry points
 * --------------------------------------------------------------- */
static int cmd_start(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
            "Usage: %s start <id> <rootfs> <cmd...>"
            " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind             = CMD_START;
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs)        - 1);

    char **cmd_argv; int cmd_argc;
    if (split_flags_and_command(&req, argc, argv, 4,
                                 &cmd_argv, &cmd_argc) != 0) return 1;
    if (cmd_argc == 0) { fprintf(stderr, "No command specified\n"); return 1; }
    pack_argv(&req, cmd_argc, cmd_argv);
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
            "Usage: %s run <id> <rootfs> <cmd...>"
            " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind             = CMD_RUN;
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs)        - 1);
    strncpy(g_run_container_id, argv[2], sizeof(g_run_container_id) - 1);

    char **cmd_argv; int cmd_argc;
    if (split_flags_and_command(&req, argc, argv, 4,
                                 &cmd_argv, &cmd_argc) != 0) return 1;
    if (cmd_argc == 0) { fprintf(stderr, "No command specified\n"); return 1; }
    pack_argv(&req, cmd_argc, cmd_argv);
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    if (argc < 3) { fprintf(stderr, "Usage: %s logs <id>\n", argv[0]); return 1; }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    if (argc < 3) { fprintf(stderr, "Usage: %s stop <id>\n", argv[0]); return 1; }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
