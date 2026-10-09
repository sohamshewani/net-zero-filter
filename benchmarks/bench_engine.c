#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define WARMUP_ITERATIONS 100000
#define BENCHMARK_ITERATIONS 10000000

/* 
 * Simulated 'net-zero-filter' interface.
 * In a real deployment, this would process packet streams or state vectors 
 * ensuring zero carbon/energy waste or neutral balance verification.
 */
typedef struct {
    uint64_t packets_processed;
    uint64_t bytes_filtered;
    uint32_t state_mask;
} NetZeroFilter;

static inline void net_zero_filter_init(NetZeroFilter *nzf) {
    nzf->packets_processed = 0;
    nzf->bytes_filtered = 0;
    nzf->state_mask = 0xA5A5A5A5;
}

static inline int net_zero_filter_process(NetZeroFilter *nzf, const uint8_t *data, size_t len) {
    // Simulate lightweight deterministic inline compute representing net-zero filtering
    uint32_t acc = nzf->state_mask;
    for (size_t i = 0; i < len; i++) {
        acc = (acc * 33) ^ data[i];
    }
    nzf->packets_processed++;
    nzf->bytes_filtered += len;
    nzf->state_mask = acc;
    return (acc & 1) ? 1 : 0;
}

static uint64_t get_nanoseconds(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int compare_uint64(const void *a, const void *b) {
    uint64_t fa = *(const uint64_t *)a;
    uint64_t fb = *(const uint64_t *)b;
    if (fa < fb) return -1;
    if (fa > fb) return 1;
    return 0;
}

int main(void) {
    NetZeroFilter nzf;
    net_zero_filter_init(&nzf);

    uint8_t sample_packet[64];
    for (int i = 0; i < 64; i++) {
        sample_packet[i] = (uint8_t)(i & 0xFF);
    }

    // Warmup phase to stabilize caches, branch predictors, and CPU frequency
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        net_zero_filter_process(&nzf, sample_packet, sizeof(sample_packet));
    }

    // Allocate latency sample array
    uint64_t *latencies = (uint64_t *)malloc(BENCHMARK_ITERATIONS * sizeof(uint64_t));
    if (!latencies) {
        fprintf(stderr, "Failed to allocate memory for latencies.\n");
        return 1;
    }

    // Reset counters for accurate measurement
    nzf.packets_processed = 0;
    nzf.bytes_filtered = 0;

    uint64_t total_start = get_nanoseconds();

    for (uint64_t i = 0; i < BENCHMARK_ITERATIONS; i++) {
        uint64_t start = get_nanoseconds();
        net_zero_filter_process(&nzf, sample_packet, sizeof(sample_packet));
        uint64_t end = get_nanoseconds();
        latencies[i] = end - start;
    }

    uint64_t total_end = get_nanoseconds();
    uint64_t total_duration_ns = total_end - total_start;

    // Sort latencies to compute percentiles
    qsort(latencies, BENCHMARK_ITERATIONS, sizeof(uint64_t), compare_uint64);

    uint64_t p50 = latencies[(size_t)(BENCHMARK_ITERATIONS * 0.50)];
    uint64_t p99 = latencies[(size_t)(BENCHMARK_ITERATIONS * 0.99)];
    uint64_t p999 = latencies[(size_t)(BENCHMARK_ITERATIONS * 0.999)];

    double duration_sec = (double)total_duration_ns / 1000000000.0;
    double ops_sec = (double)BENCHMARK_ITERATIONS / duration_sec;

    // Output formatting
    printf("+--------------------------+-----------------------+\n");
    printf("| Metric                   | Value                 |\n");
    printf("+--------------------------+-----------------------+\n");
    printf("| Total Iterations         | %-21lu |\n", (unsigned long)BENCHMARK_ITERATIONS);
    printf("| Total Duration (sec)     | %-21.6f |\n", duration_sec);
    printf("| Throughput (ops/sec)     | %-21.2f |\n", ops_sec);
    printf("| Latency P50              | %-17lu ns |\n", (unsigned long)p50);
    printf("| Latency P99              | %-17lu ns |\n", (unsigned long)p99);
    printf("| Latency P99.9            | %-17lu ns |\n", (unsigned long)p999);
    printf("+--------------------------+-----------------------+\n");

    free(latencies);
    return 0;
}