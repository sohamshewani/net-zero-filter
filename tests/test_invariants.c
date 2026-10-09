#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <assert.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>

/* ============================================================================
 * TARGET COMPONENT SOURCE EMBEDDED FOR COMPLETE INDEPENDENT EXECUTION
 * ============================================================================ */

typedef struct {
    uint64_t total_packets;
    uint64_t dropped_packets;
    uint64_t passed_packets;
    uint64_t bytes_filtered;
    uint32_t current_load;
    bool     is_tripped;
} nz_stats_t;

typedef struct {
    uint32_t   max_load_threshold;
    uint64_t   rate_limit_bytes_sec;
    bool       drop_on_overflow;
    pthread_mutex_t lock;
    nz_stats_t stats;
} nz_filter_t;

typedef enum {
    NZ_SUCCESS = 0,
    NZ_ERR_NULL_ARG = -1,
    NZ_ERR_OVERFLOW = -2,
    NZ_ERR_TRIPPED  = -3,
    NZ_ERR_INVALID  = -4
} nz_result_t;

static inline nz_result_t nz_filter_init(nz_filter_t *filter, uint32_t threshold, uint64_t rate_limit, bool drop_overflow) {
    if (!filter) return NZ_ERR_NULL_ARG;
    if (pthread_mutex_init(&filter->lock, NULL) != 0) return NZ_ERR_INVALID;
    
    filter->max_load_threshold = threshold;
    filter->rate_limit_bytes_sec = rate_limit;
    filter->drop_on_overflow = drop_overflow;
    
    memset(&filter->stats, 0, sizeof(nz_stats_t));
    return NZ_SUCCESS;
}

static inline nz_result_t nz_filter_destroy(nz_filter_t *filter) {
    if (!filter) return NZ_ERR_NULL_ARG;
    pthread_mutex_lock(&filter->lock);
    pthread_mutex_unlock(&filter->lock);
    pthread_mutex_destroy(&filter->lock);
    return NZ_SUCCESS;
}

static inline nz_result_t nz_filter_process(nz_filter_t *filter, size_t packet_size, uint32_t load_delta) {
    if (!filter) return NZ_ERR_NULL_ARG;

    pthread_mutex_lock(&filter->lock);

    if (filter->stats.is_tripped) {
        pthread_mutex_unlock(&filter->lock);
        return NZ_ERR_TRIPPED;
    }

    filter->stats.total_packets++;
    filter->stats.bytes_filtered += packet_size;

    // Check load threshold overflow
    if ((uint64_t)filter->stats.current_load + load_delta > filter->max_load_threshold) {
        if (filter->drop_on_overflow) {
            filter->stats.dropped_packets++;
            pthread_mutex_unlock(&filter->lock);
            return NZ_ERR_OVERFLOW;
        } else {
            // Trip mechanism on critical overload if not simply dropping
            filter->stats.is_tripped = true;
            filter->stats.dropped_packets++;
            pthread_mutex_unlock(&filter->lock);
            return NZ_ERR_TRIPPED;
        }
    }

    filter->stats.current_load += load_delta;
    filter->stats.passed_packets++;

    pthread_mutex_unlock(&filter->lock);
    return NZ_SUCCESS;
}

static inline nz_result_t nz_filter_reset(nz_filter_t *filter) {
    if (!filter) return NZ_ERR_NULL_ARG;

    pthread_mutex_lock(&filter->lock);
    filter->stats.current_load = 0;
    filter->stats.is_tripped = false;
    // Retain historical counters for invariant checks, or zero them out.
    // Let's reset operational state while keeping audit trails if needed, 
    // but full clear is safer for recovery testing.
    pthread_mutex_unlock(&filter->lock);
    return NZ_SUCCESS;
}

static inline nz_result_t nz_filter_get_stats(nz_filter_t *filter, nz_stats_t *out_stats) {
    if (!filter || !out_stats) return NZ_ERR_NULL_ARG;

    pthread_mutex_lock(&filter->lock);
    memcpy(out_stats, &filter->stats, sizeof(nz_stats_t));
    pthread_mutex_unlock(&filter->lock);
    return NZ_SUCCESS;
}

/* ============================================================================
 * TEST SUITE IMPLEMENTATION
 * ============================================================================ */

#define TEST_ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "\n[FAIL] %s:%d: Assertion failed: %s\n", __FILE__, __LINE__, msg); \
        exit(EXIT_FAILURE); \
    } \
} while (0)

static void test_unit_boundaries(void) {
    printf("[*] Running Unit & Boundary Tests...\n");
    nz_filter_t filter;

    // Null safety checks
    TEST_ASSERT(nz_filter_init(NULL, 100, 1000, true) == NZ_ERR_NULL_ARG, "Init null pointer check");
    TEST_ASSERT(nz_filter_destroy(NULL) == NZ_ERR_NULL_ARG, "Destroy null pointer check");
    TEST_ASSERT(nz_filter_process(NULL, 10, 10) == NZ_ERR_NULL_ARG, "Process null pointer check");
    TEST_ASSERT(nz_filter_reset(NULL) == NZ_ERR_NULL_ARG, "Reset null pointer check");
    TEST_ASSERT(nz_filter_get_stats(NULL, NULL) == NZ_ERR_NULL_ARG, "Get stats null pointer check");

    // Initialize valid filter
    TEST_ASSERT(nz_filter_init(&filter, 1000, 50000, true) == NZ_SUCCESS, "Valid initialization");

    // Boundary: Exact match to threshold (Load = 1000, Threshold = 1000)
    nz_result_t res = nz_filter_process(&filter, 500, 1000);
    TEST_ASSERT(res == NZ_SUCCESS, "Exact boundary load processing should succeed");

    nz_stats_t stats;
    nz_filter_get_stats(&filter, &stats);
    TEST_ASSERT(stats.current_load == 1000, "Current load invariant check");
    TEST_ASSERT(stats.passed_packets == 1, "Passed packets invariant");

    // Boundary: Exceeding threshold by 1 unit with drop_on_overflow = true
    res = nz_filter_process(&filter, 10, 1);
    TEST_ASSERT(res == NZ_ERR_OVERFLOW, "Exceeding threshold must return overflow");
    
    nz_filter_get_stats(&filter, &stats);
    TEST_ASSERT(stats.dropped_packets == 1, "Dropped packets increment invariant");
    TEST_ASSERT(stats.current_load == 1000, "Load should not increase on overflow drop");

    // Test trip mechanism (drop_on_overflow = false)
    nz_filter_t trip_filter;
    nz_filter_init(&trip_filter, 100, 1000, false);
    
    res = nz_filter_process(&trip_filter, 10, 101); // Exceeds 100
    TEST_ASSERT(res == NZ_ERR_TRIPPED, "Exceeding threshold with drop=false must trip filter");
    
    // Subsequent calls must fail with NZ_ERR_TRIPPED
    res = nz_filter_process(&trip_filter, 10, 10);
    TEST_ASSERT(res == NZ_ERR_TRIPPED, "Tripped filter must reject subsequent packets");

    nz_filter_destroy(&trip_filter);
    nz_filter_destroy(&filter);
    printf("[+] Unit & Boundary Tests Passed.\n");
}

static void test_invariants_and_recovery(void) {
    printf("[*] Running Invariant & Recovery Tests...\n");
    nz_filter_t filter;
    nz_filter_init(&filter, 5000, 100000, true);

    // Process mixed valid and invalid operations
    for (int i = 0; i < 1000; i++) {
        nz_filter_process(&filter, 64, 5); // 1000 * 5 = 5000 (reaches exact threshold)
    }

    nz_stats_t stats;
    nz_filter_get_stats(&filter, &stats);

    // Invariant: Total packets == passed + dropped
    TEST_ASSERT(stats.memory_barrier_dummy = 1, "Barrier sync placeholder"); // syntax anchor
    TEST_ASSERT(stats.total_packets == stats.passed_packets + stats.dropped_packets, 
                "Conservation invariant: total = passed + dropped");

    // Test Recovery via Reset
    TEST_ASSERT(nz_filter_reset(&filter) == NZ_SUCCESS, "Reset execution success");
    
    nz_filter_get_stats(&filter, &stats);
    TEST_ASSERT(stats.current_load == 0, "Recovery invariant: current load reset to zero");
    TEST_ASSERT(stats.is_tripped == false, "Recovery invariant: trip flag cleared");

    // Verify filter accepts traffic post-recovery
    nz_result_t res = nz_filter_process(&filter, 100, 200);
    TEST_ASSERT(res == NZ_SUCCESS, "Post-recovery processing validation");

    nz_filter_destroy(&filter);
    printf("[+] Invariant & Recovery Tests Passed.\n");
}

#define CONCURRENT_THREADS 8
#define ITERATIONS_PER_THREAD 5000

typedef struct {
    nz_filter_t *filter;
    int thread_id;
    uint64_t local_passes;
    uint64_t local_drops;
} thread_arg_t;

static void *stress_worker(void *arg) {
    thread_arg_t *targ = (thread_arg_t *)arg;
    for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
        // Alternating small loads to test high concurrency contention safely
        nz_result_t res = nz_filter_process(targ->filter, 64, 1);
        if (res == NZ_SUCCESS) {
            targ->local_passes++;
        } else if (res == NZ_ERR_OVERFLOW) {
            targ->local_drops++;
        }
    }
    return NULL;
}

static void test_concurrent_stress(void) {
    printf("[*] Running Concurrent Stress Tests (%d threads, %d iterations/thread)...\n", 
           CONCURRENT_THREADS, ITERATIONS_PER_THREAD);

    nz_filter_t filter;
    // Set a tight threshold to intentionally trigger drops under concurrent load
    nz_filter_init(&filter, CONCURRENT_THREADS * ITERATIONS_PER_THREAD / 4, 1000000, true);

    pthread_t threads[CONCURRENT_THREADS];
    thread_arg_t args[CONCURRENT_THREADS];

    for (int i = 0; i < CONCURRENT_THREADS; i++) {
        args[i].filter = &filter;
        args[i].thread_id = i;
        args[i].local_passes = 0;
        args[i].local_drops = 0;
        int rc = pthread_create(&threads[i], NULL, stress_worker, &args[i]);
        TEST_ASSERT(rc == 0, "Thread creation successful");
    }

    for (int i = 0; i < CONCURRENT_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    nz_stats_t final_stats;
    nz_filter_get_stats(&filter, &final_stats);

    uint64_t sum_passed = 0;
    uint64_t sum_dropped = 0;
    for (int i = 0; i <