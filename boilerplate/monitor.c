
/*
monitor.c - Kernel module for monitoring container memory usage and enforcing limits
*/
#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME        "container_monitor"
#define CHECK_INTERVAL_SEC 1


struct monitored_entry {
    pid_t          pid;
    char           container_id[MONITOR_NAME_LEN];
    unsigned long  soft_limit_bytes;
    unsigned long  hard_limit_bytes;
    int            soft_warned;
    struct list_head node;
};


static LIST_HEAD(monitored_list);
static DEFINE_MUTEX(monitored_lock);


static struct timer_list monitor_timer;
static dev_t             dev_num;
static struct cdev       c_dev;
static struct class     *cl;


/*
================================================================================
TASK 4: MEASURE PROCESS RESIDENT SET SIZE (RSS)
================================================================================
Queried by kernel timer callback every 1 second to monitor memory usage.

Algorithm:
  1. Find task_struct for given pid using kernel pid lookup
  2. Get memory descriptor (mm_struct) from task
  3. Call get_mm_rss(): returns resident pages in task's page tables
  4. Multiply by PAGE_SIZE to convert pages to bytes
  5. Release references and return byte count

Returns: RSS in bytes, or -1 if process no longer exists

Why measure RSS? Actual physical memory used (excludes swapped pages).
Virtual size (VSZ) would include mmap'd files, swapped memory, etc.
*/
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct   *mm;
    long                rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) { rcu_read_unlock(); return -1; }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) { rss_pages = get_mm_rss(mm); mmput(mm); }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}


/*
================================================================================
TASK 4: LOG SOFT LIMIT WARNING
================================================================================
Called when container first exceeds soft_limit_bytes (but not hard limit).
Logged once per container via soft_warned flag (prevents log spam).

Output goes to kernel log (dmesg) with KERN_WARNING level:
  [container_monitor] SOFT LIMIT container=<id> pid=<pid> rss=<bytes> limit=<bytes>

Operator checks dmesg to see which containers are approaching memory limits
and can take corrective action (e.g., increase limits, optimize app).
*/
static void log_soft_limit_event(const char *container_id,
                                  pid_t pid,
                                  unsigned long limit_bytes,
                                  long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d"
           " rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}


/*
================================================================================
TASK 4: ENFORCE HARD LIMIT - SEND SIGKILL TO CONTAINER
================================================================================
Called when container's RSS exceeds hard_limit_bytes.
Kernel sends SIGKILL (signal 9) which cannot be caught or ignored.

Algorithm:
  1. Use RCU (Read-Copy-Update) locking to safely access task_struct
  2. Find task_struct for given pid
  3. Call send_sig(SIGKILL): Sends signal 9 (termination)
  4. Log event to kernel log at KERN_WARNING level

Result: Container process is terminated immediately by kernel.
Engine.c receives SIGCHLD, updates container state to CONTAINER_HARD_LIMIT_KILLED,
and passes this info to user in "engine ps" output (TASK 2 requirement).
*/
static void kill_process(const char *container_id,
                          pid_t pid,
                          unsigned long limit_bytes,
                          long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task) send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d"
           " rss=%ld limit=%lu — SIGKILL sent\n",
           container_id, pid, rss_bytes, limit_bytes);
}


/*
================================================================================
TASK 4: TIMER CALLBACK - PERIODIC MEMORY MONITORING
================================================================================
Called by kernel every CHECK_INTERVAL_SEC (1 second).
Monitors all registered containers and enforces memory limits.

Algorithm:
  1. Acquire mutex (prevents concurrent modification of monitored_list)
  2. For each monitored container:
     a. Call get_rss_bytes() to measure current memory
     b. If rss < 0: process exited, remove entry
     c. If rss > hard_limit: SIGKILL + remove entry
     d. If rss > soft_limit && !soft_warned: log warning, set flag
  3. Release mutex
  4. Re-arm timer for next callback (jiffies + 1 second)

Thread-safety: Runs in softirq/timer context (not preemptible).
Mutex protects list from concurrent ioctl calls (register/unregister).

Design note: list_for_each_entry_safe() allows safe deletion during iteration.
*/
static void timer_callback(struct timer_list *t)
{
    struct monitored_entry *entry, *tmp;

    mutex_lock(&monitored_lock);

    list_for_each_entry_safe(entry, tmp, &monitored_list, node) {
        long rss = get_rss_bytes(entry->pid);


        if (rss < 0) {
            printk(KERN_INFO
                   "[container_monitor] container=%s pid=%d exited,"
                   " removing entry\n",
                   entry->container_id, entry->pid);
            list_del(&entry->node);
            kfree(entry);
            continue;
        }


        if ((unsigned long)rss > entry->hard_limit_bytes) {
            kill_process(entry->container_id, entry->pid,
                         entry->hard_limit_bytes, rss);
            list_del(&entry->node);
            kfree(entry);
            continue;
        }


        if ((unsigned long)rss > entry->soft_limit_bytes &&
            !entry->soft_warned) {
            entry->soft_warned = 1;
            log_soft_limit_event(entry->container_id, entry->pid,
                                 entry->soft_limit_bytes, rss);
        }
    }

    mutex_unlock(&monitored_lock);

    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}


/*
================================================================================
TASK 4: IOCTL INTERFACE - REGISTER/UNREGISTER CONTAINERS
================================================================================
Called from user-space via ioctl(fd, MONITOR_REGISTER|UNREGISTER, &req).
Main interface between engine.c and kernel module.

Two operations:

1. MONITOR_REGISTER (engine.c: launch_container):
   a. Copy ioctl argument from user space (copy_from_user)
   b. Validate soft_limit <= hard_limit
   c. Allocate monitored_entry structure
   d. Initialize with pid, container_id, limits, soft_warned=0
   e. Add to monitored_list (protected by mutex)
   f. Return 0 (success)
   → Kernel timer begins monitoring this container's memory

2. MONITOR_UNREGISTER (engine.c: sigchld_handler):
   a. Copy ioctl argument from user space
   b. Search monitored_list for matching (pid, container_id)
   c. If found: remove from list, free memory, return 0
   d. If not found: return -ENOENT
   → Kernel timer stops monitoring (entry removed from list)

Error handling:
  - -EINVAL: Invalid command, or soft_limit > hard_limit
  - -EFAULT: copy_from_user failed (security issue)
  - -ENOMEM: Allocation failed
  - -ENOENT: Container not found in monitored_list

Thread-safety: Mutex protects against concurrent timer callbacks.
*/
static long monitor_ioctl(struct file *f,
                           unsigned int cmd,
                           unsigned long arg)
{
    struct monitor_request req;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req,
                       (struct monitor_request __user *)arg,
                       sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {

        struct monitored_entry *entry;

        printk(KERN_INFO
               "[container_monitor] Register container=%s pid=%d"
               " soft=%lu hard=%lu\n",
               req.container_id, req.pid,
               req.soft_limit_bytes, req.hard_limit_bytes);

        if (req.soft_limit_bytes > req.hard_limit_bytes) {
            printk(KERN_ERR
                   "[container_monitor] soft > hard for container=%s\n",
                   req.container_id);
            return -EINVAL;
        }

        entry = kzalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry) return -ENOMEM;

        entry->pid              = req.pid;
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_warned      = 0;
        strncpy(entry->container_id, req.container_id,
                sizeof(entry->container_id) - 1);
        INIT_LIST_HEAD(&entry->node);

        mutex_lock(&monitored_lock);
        list_add_tail(&entry->node, &monitored_list);
        mutex_unlock(&monitored_lock);

        return 0;
    }


    {
        struct monitored_entry *entry, *tmp;
        int found = 0;

        printk(KERN_INFO
               "[container_monitor] Unregister container=%s pid=%d\n",
               req.container_id, req.pid);

        mutex_lock(&monitored_lock);
        list_for_each_entry_safe(entry, tmp, &monitored_list, node) {
            if (entry->pid == req.pid &&
                strncmp(entry->container_id,
                        req.container_id,
                        MONITOR_NAME_LEN) == 0) {
                list_del(&entry->node);
                kfree(entry);
                found = 1;
                break;
            }
        }
        mutex_unlock(&monitored_lock);

        return found ? 0 : -ENOENT;
    }
}


static struct file_operations fops = {
    .owner          = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};


/*
================================================================================
TASK 4, 6: KERNEL MODULE INITIALIZATION
================================================================================
Called when module is loaded via insmod/modprobe.
Sets up character device and initializes periodic monitoring timer.

Steps:
  1. alloc_chrdev_region(): Allocate major/minor device numbers dynamically
  2. class_create(): Create device class in /sys/class/<name>
  3. device_create(): Create /dev/container_monitor file
  4. cdev_init(): Initialize character device with fops (file operations)
  5. cdev_add(): Register character device with kernel
  6. timer_setup(): Initialize kernel timer for periodic monitoring
  7. mod_timer(): Arm timer to fire every 1 second

Error handling: If any step fails, unwind (destroy/unregister previous steps)
to avoid resource leaks or kernel panics.

Result: /dev/container_monitor ready for user-space ioctl calls.
Timer begins callbacks every 1 second (timer_callback).

Returns: 0 on success, -1 or PTR_ERR on failure
*/
static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO
           "[container_monitor] Module loaded. Device: /dev/%s\n",
           DEVICE_NAME);
    return 0;
}


/*
================================================================================
TASK 6: KERNEL MODULE CLEANUP AND SHUTDOWN
================================================================================
Called when module is unloaded via rmmod.
Cleans up all resources and gracefully stops monitoring.

Steps:
  1. timer_delete_sync(): Stop timer and wait for in-flight callbacks
  2. Walk monitored_list: Free all tracked containers
  3. cdev_del(): Unregister character device
  4. device_destroy(): Delete /dev/container_monitor file
  5. class_destroy(): Remove device class from sysfs
  6. unregister_chrdev_region(): Release device numbers

Order matters: Must delete timer BEFORE freeing monitored_list (timer callback
could crash if list is freed while timer is still running).

Robustness: timer_delete_sync() guarantees no pending callbacks.
Mutex used when freeing list (defensive, though no other threads should exist).

Result: Module fully unloaded, all resources released to kernel.
No dangling pointers, file descriptors, or device nodes.
*/
static void __exit monitor_exit(void)
{
    struct monitored_entry *entry, *tmp;

    timer_delete_sync(&monitor_timer);


    mutex_lock(&monitored_lock);
    list_for_each_entry_safe(entry, tmp, &monitored_list, node) {
        printk(KERN_INFO
               "[container_monitor] Freeing entry container=%s pid=%d\n",
               entry->container_id, entry->pid);
        list_del(&entry->node);
        kfree(entry);
    }
    mutex_unlock(&monitored_lock);

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");

