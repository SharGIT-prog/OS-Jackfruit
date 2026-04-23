/*
 * cpu_hog.c - CPU-bound workload for scheduler experiments.
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>


#define CHECK_ITERS 10000000ULL

static unsigned int parse_seconds(const char *arg, unsigned int fallback)
{
    char *end = NULL;
    unsigned long v;
    if (!arg || *arg == '\0') return fallback;
    v = strtoul(arg, &end, 10);
    if (end && *end != '\0') return fallback;
    if (v == 0) return fallback;
    return (unsigned int)v;
}

/* Nanoseconds of CPU time consumed by this process. */
static long long cpu_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

int main(int argc, char *argv[])
{
    const unsigned int duration = (argc > 1) ? parse_seconds(argv[1], 10) : 10;
    volatile unsigned long long accumulator = 0;

    struct timespec wall_start, wall_now;
    clock_gettime(CLOCK_MONOTONIC, &wall_start);

    long long cpu_start      = cpu_ns();
    long long next_report_ns = cpu_start + 1000000000LL; /* first tick at 1 CPU-second */

    while (1) {
        /* Pure CPU burn — no syscalls inside the hot loop */
        unsigned long long i;
        for (i = 0; i < CHECK_ITERS; i++)
            accumulator = accumulator * 6364136223846793005ULL
                        + 1442695040888963407ULL;

        /* Emit one line per CPU-second consumed */
        long long now_cpu = cpu_ns();
        if (now_cpu >= next_report_ns) {
            clock_gettime(CLOCK_MONOTONIC, &wall_now);
            long wall_elapsed = wall_now.tv_sec - wall_start.tv_sec;
            printf("cpu_hog alive elapsed=%ld accumulator=%llu\n",
                   wall_elapsed, accumulator);
            fflush(stdout);
            next_report_ns += 1000000000LL;
        }

        /* Exit after wall-clock duration */
        clock_gettime(CLOCK_MONOTONIC, &wall_now);
        if ((unsigned int)(wall_now.tv_sec - wall_start.tv_sec) >= duration)
            break;
    }

    printf("cpu_hog done duration=%u accumulator=%llu\n", duration, accumulator);
    fflush(stdout);
    return 0;
}
