#ifndef NET_TYPES_H
#define NET_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <time.h>
#include <string.h>
#include <sys/types.h>

#define NET_CACHE_LINE_SIZE 64
#define NET_MAX_FLOW_KEY 16
#define NET_DEFAULT_ARENA_SIZE (1024 * 1024 * 64) // 64MB

typedef enum {
    NET_SUCCESS = 0,
    NET_ERR_INVALID_ARG = -1,
    NET_ERR_RING_FULL = -2,
    NET_ERR_RING_EMPTY = -3,
    NET_ERR_ARENA_EXHAUSTED = -4,
    NET_ERR_FLOW_NOT_FOUND = -5,
    NET_ERR_FLOW_EXISTS = -6,
    NET_ERR_OUT_OF_BOUNDS = -7
} NetStatus;

typedef enum {
    NET_BACKPRESSURE_DROP = 0,
    NET_BACKPRESSURE_THROTTLE = 1
} NetBackpressurePolicy;

typedef struct _Alignas(NET_CACHE_LINE_SIZE) NetPacket {
    uint8_t *data;
    uint32_t length;
    uint32_t capacity;
    uint64_t timestamp_ns;
    uint64_t flow_id;
    uint32_t ingress_ifindex;
    uint16_t flags;
    uint16_t reserved;
} NetPacket;

typedef struct _Alignas(NET_CACHE_LINE_SIZE) NetRingSlot {
    atomic_uint_fast64_t sequence;
    NetPacket *packet;
} NetRingSlot;

typedef struct _Alignas(NET_CACHE_LINE_SIZE) NetRing {
    size_t capacity;
    size_t mask;
    NetRingSlot *slots;
    _Alignas(NET_CACHE_LINE_SIZE) atomic_size_t head;
    _Alignas(NET_CACHE_LINE_SIZE) atomic_size_t tail;
    NetBackpressurePolicy bp_policy;
    atomic_uint_fast64_t dropped_packets;
} NetRing;

typedef struct _Alignas(NET_CACHE_LINE_SIZE) NetArena {
    uint8_t *buffer;
    size_t capacity;
    _Alignas(NET_CACHE_LINE_SIZE) atomic_size_t offset;
    uint64_t epoch;
} NetArena;

typedef struct _Alignas(NET_CACHE_LINE_SIZE) NetFlowEntry {
    uint64_t flow_id;
    uint8_t key[NET_MAX_FLOW_KEY];
    uint8_t key_len;
    uint32_t probe_dist;
    uint64_t last_seen_ns;
    uint64_t packet_count;
    uint64_t byte_count;
    bool active;
} NetFlowEntry;

typedef struct _Alignas(NET_CACHE_LINE_SIZE) NetFlowTracker {
    NetFlowEntry *entries;
    size_t capacity;
    size_t mask;
    atomic_size_t active_count;
    uint64_t window_size_ns;
} NetFlowTracker;

static inline bool net_is_power_of_two(size_t n) {
    return (n != 0) && ((n & (n - 1)) == 0);
}

static inline uint64_t net_current_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline NetStatus net_ring_init(NetRing *ring, size_t capacity, NetBackpressurePolicy bp_policy) {
    if (!ring || !net_is_power_of_two(capacity)) {
        return NET_ERR_INVALID_ARG;
    }
    
    posix_memalign((void **)&ring->slots, NET_CACHE_LINE_SIZE, sizeof(NetRingSlot) * capacity);
    if (!ring->slots) {
        return NET_ERR_INVALID_ARG;
    }

    ring->capacity = capacity;
    ring->mask = capacity - 1;
    ring->bp_policy = bp_policy;
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    atomic_init(&ring->dropped_packets, 0);

    for (size_t i = 0; i < capacity; i++) {
        atomic_init(&ring->slots[i].sequence, i);
        ring->slots[i].packet = NULL;
    }

    return NET_SUCCESS;
}

static inline void net_ring_free(NetRing *ring) {
    if (ring && ring->slots) {
        free(ring->slots);
        ring->slots = NULL;
        ring->capacity = 0;
        ring->mask = 0;
    }
}

static inline NetStatus net_ring_enqueue(NetRing *ring, NetPacket *packet) {
    NetRingSlot *slot;
    size_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);

    for (;;) {
        slot = &ring->slots[head & ring->mask];
        uint64_t seq = atomic_load_explicit(&slot->sequence, memory_order_acquire);
        intptr_t diff = (intptr_t)seq - (intptr_t)head;

        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(&ring->head, &head, head + 1,
                                                      memory_order_relaxed, memory_order_relaxed)) {
                break;
            }
        } else if (diff < 0) {
            if (ring->bp_policy == NET_BACKPRESSURE_DROP) {
                atomic_fetch_add_explicit(&ring->dropped_packets, 1, memory_order_relaxed);
                return NET_ERR_RING_FULL;
            }
            return NET_ERR_RING_FULL;
        } else {
            head = atomic_load_explicit(&ring->head, memory_order_relaxed);
        }
    }

    slot->packet = packet;
    atomic_store_explicit(&slot->sequence, head + 1, memory_order_release);
    return NET_SUCCESS;
}

static inline NetStatus net_ring_dequeue(NetRing *ring, NetPacket **packet) {
    NetRingSlot *slot;
    size_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);

    for (;;) {
        slot = &ring->slots[tail & ring->mask];
        uint64_t seq = atomic_load_explicit(&slot->sequence, memory_order_acquire);
        intptr_t diff = (intptr_t)seq - (intptr_t)(tail + 1);

        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(&ring->tail, &tail, tail + 1,
                                                      memory_order_relaxed, memory_order_relaxed)) {
                break;
            }
        } else if (diff < 0) {
            return NET_ERR_RING_EMPTY;
        } else {
            tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
        }
    }

    *packet = slot->packet;
    slot->packet = NULL;
    atomic_store_explicit(&slot->sequence, tail + ring->capacity, memory_order_release);
    return NET_SUCCESS;
}

static inline NetStatus net_arena_init(NetArena *arena, size_t capacity) {
    if (!arena || capacity == 0) {
        return NET_ERR_INVALID_ARG;
    }

    int res = posix_memalign((void **)&arena->buffer, NET_CACHE_LINE_SIZE, capacity);
    if (res != 0 || !arena->buffer) {
        return NET_ERR_INVALID_ARG;
    }

    arena->capacity = capacity;
    atomic_init(&arena->offset, 0);
    arena->epoch = net_current_time_ns();
    memset(arena->buffer, 0, capacity);

    return NET_SUCCESS;
}

static inline void net_arena_free(NetArena *arena) {
    if (arena && arena->buffer) {
        free(arena->buffer);
        arena->buffer = NULL;
        arena->capacity = 0;
        atomic_store_explicit(&arena->offset, 0, memory_order_relaxed);
    }
}

static inline void *net_arena_alloc(NetArena *arena, size_t size) {
    if (!arena || size == 0) {
        return NULL;
    }

    size_t aligned_size = (size + (NET_CACHE_LINE_SIZE - 1)) & ~(NET_CACHE_LINE_SIZE - 1);
    size_t current = atomic_load_explicit(&arena->offset, memory_order_relaxed);

    for (;;) {
        if (current + aligned_size > arena->capacity) {
            return NULL;
        }
        if (atomic_compare_exchange_weak_explicit(&arena->offset, &current, current + aligned_size,
                                                  memory_order_relaxed, memory_order_relaxed)) {
            return (void *)(arena->buffer + current);
        }
    }
}

static inline void net_arena_reset(NetArena *arena) {
    if (arena) {
        atomic_store_explicit(&arena->offset, 0, memory_order_release);
        arena->epoch = net_current_time_ns();
    }
}

static inline uint64_t net_hash_key(const uint8_t *key, size_t len) {
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint64_t)key[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

static inline NetStatus net_flow_tracker_init(NetFlowTracker *tracker, size_t capacity, uint64_t window_size_ns) {
    if (!tracker || !net_is_power_of_two(capacity)) {
        return NET_ERR_INVALID_ARG;
    }

    int res = posix_memalign((void **)&tracker->entries, NET_CACHE_LINE_SIZE, sizeof(NetFlowEntry) * capacity);
    if (res != 0 || !tracker->entries) {
        return NET_ERR_INVALID_ARG;
    }

    tracker->capacity = capacity;
    tracker->mask = capacity - 1;
    tracker->window_size_ns = window_size_ns;
    atomic_init(&tracker->active_count, 0);

    for (size_t i = 0; i < capacity; i++) {
        tracker->entries[i].flow_id = 0;
        tracker->entries[i].key_len = 0;
        tracker->entries[i].probe_dist = 0;
        tracker->entries[i].last_seen_ns = 0;
        tracker->entries[i].packet_count = 0;
        tracker->entries[i].byte_count = 0;
        tracker->entries[i].active = false;
        memset(tracker->entries[i].key, 0, NET_MAX_FLOW_KEY);
    }

    return NET_SUCCESS;
}

static inline void net_flow_tracker_free(NetFlowTracker *tracker) {
    if (tracker && tracker->entries) {
        free(tracker->entries);
        tracker->entries = NULL;
        tracker->capacity = 0;
        tracker->mask = 0;
    }
}

static inline NetStatus net_flow_tracker_update(NetFlowTracker *tracker, const uint8_t *key, size_t key_len, uint64_t pkt_len, uint64_t now_ns) {
    if (!tracker || !key || key_len == 0 || key_len > NET_MAX_FLOW_KEY) {
        return NET_ERR_INVALID_ARG;
    }

    uint64_t flow_id = net_hash_key(key, key_len);
    size_t idx = flow_id & tracker->mask;
    uint32_t probe_dist = 0;

    NetFlowEntry candidate = {
        .flow_id = flow_id,
        .key_len = (uint8_t)key_len,
        .probe_dist = 0,
        .last_seen_ns = now_ns,
        .packet_count = 1,
        .byte_count = pkt_len,
        .active = true
    };
    memcpy(candidate.key, key, key_len);

    for (;;) {
        NetFlowEntry *current = &tracker->entries[idx];

        if (!current->active) {
            candidate.probe_dist = probe_dist;
            *current = candidate;
            atomic_fetch_add_explicit(&tracker->active_count, 1, memory_order_relaxed);
            return NET_SUCCESS;
        }

        if (current->flow_id == flow_id && current->key_len == key_len && memcmp(current->key, key, key_len) == 0) {
            if (now_ns - current->last_seen_ns > tracker->window_size_ns) {
                current->packet_count = 1;
                current->byte_count = pkt_len;
            } else {
                current->packet_count++;
                current->byte_count += pkt_len;
            }
            current->last_seen_ns = now_ns;
            return NET_SUCCESS;
        }

        if (probe_dist > current->probe_dist) {
            NetFlowEntry temp = *current;
            candidate.probe_dist = probe_dist;
            *current = candidate;
            candidate = temp;
            probe_dist = candidate.probe_dist;
        }

        probe_dist++;
        idx = (idx + 1) & tracker->mask;

        if (probe_dist >= tracker->capacity) {
            return NET_ERR_ARENA_EXHAUSTED;
        }
    }
}

static inline void net_flow_tracker_gc(NetFlowTracker *tracker, uint64_t now_ns) {
    if (!tracker) return;

    for (size_t i = 0; i < tracker->capacity; i++) {
        NetFlowEntry *entry = &tracker->entries[i];
        if (entry->active && (now_ns - entry->last_seen_ns > tracker->window_size_ns)) {
            entry->active = false;
            entry->flow_id = 0;
            entry->key_len = 0;
            entry->probe_dist = 0;
            atomic_fetch_sub_explicit(&tracker->active_count, 1, memory_order_relaxed);
        }
    }
}

#endif /* NET_TYPES_H */