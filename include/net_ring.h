#ifndef NET_RING_H
#define NET_RING_H

#_Pragma("once")

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <string.h>
#include <sys/types.h>

#define NET_CACHE_LINE_SIZE 64
#define NET_ALIGNAS _Alignas(NET_CACHE_LINE_SIZE)

typedef enum {
    NET_SUCCESS = 0,
    NET_ERR_INVALID_ARG = -1,
    NET_ERR_RING_FULL = -2,
    NET_ERR_RING_EMPTY = -3,
    NET_ERR_ARENA_EXHAUSTED = -4,
    NET_ERR_FLOW_EXISTS = -5,
    NET_ERR_FLOW_NOT_FOUND = -6,
    NET_ERR_OUT_OF_BOUNDS = -7
} NetError;

typedef struct NET_ALIGNAS {
    uint8_t *data;
    uint32_t capacity;
    uint32_t len;
    uint64_t timestamp_ns;
    uint32_t flow_id;
    uint16_t flags;
    uint16_t reserved;
} NetPacket;

typedef struct NET_ALIGNAS {
    _Atomic uint32_t head;
    char pad1[NET_CACHE_LINE_SIZE - sizeof(_Atomic uint32_t)];
    _Atomic uint32_t tail;
    char pad2[NET_CACHE_LINE_SIZE - sizeof(_Atomic uint32_t)];
    uint32_t capacity_mask;
    uint32_t element_size;
    NetPacket *buffer;
} NetRing;

typedef struct NET_ALIGNAS {
    uint8_t *memory_pool;
    size_t pool_size;
    _Atomic size_t offset;
    char pad[NET_CACHE_LINE_SIZE - sizeof(size_t) - sizeof(size_t) - sizeof(_Atomic size_t)];
} NetArena;

typedef struct {
    uint32_t flow_id;
    uint64_t last_seen_ns;
    uint32_t packet_count;
    uint8_t active;
    uint8_t robin_hood_dib;
} NetFlowEntry;

typedef struct NET_ALIGNAS {
    NetFlowEntry *entries;
    uint32_t capacity;
    uint32_t mask;
    _Atomic uint32_t active_flows;
} NetFlowTracker;

static inline bool net_is_power_of_two(uint32_t x) {
    return x != 0 && (x & (x - 1)) == 0;
}

static inline NetError net_ring_init(NetRing *ring, uint32_t capacity, NetPacket *storage) {
    if (!ring || !storage || !net_is_power_of_two(capacity)) {
        return NET_ERR_INVALID_ARG;
    }
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    ring->capacity_mask = capacity - 1;
    ring->element_size = sizeof(NetPacket);
    ring->buffer = storage;
    memset(ring->buffer, 0, capacity * sizeof(NetPacket));
    return NET_SUCCESS;
}

static inline NetError net_ring_enqueue(NetRing *ring, const NetPacket *packet) {
    if (!ring || !packet) {
        return NET_ERR_INVALID_ARG;
    }

    uint32_t current_head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint32_t current_tail = atomic_load_explicit(&ring->tail, memory_order_acquire);

    if ((current_head - current_tail) > ring->capacity_mask) {
        return NET_ERR_RING_FULL;
    }

    while (true) {
        if ((current_head - current_tail) > ring->capacity_mask) {
            return NET_ERR_RING_FULL;
        }

        if (atomic_compare_exchange_weak_explicit(&ring->head, &current_head, current_head + 1,
                                                  memory_order_release, memory_order_relaxed)) {
            break;
        }
        current_tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    }

    uint32_t index = current_head & ring->capacity_mask;
    memcpy(&ring->buffer[index], packet, sizeof(NetPacket));

    uint32_t expected_tail = current_head;
    while (!atomic_compare_exchange_weak_explicit(&ring->tail, &expected_tail, current_head + 1,
                                                  memory_order_release, memory_order_relaxed)) {
        expected_tail = current_head;
        sched_yield();
    }

    return NET_SUCCESS;
}

static inline NetError net_ring_dequeue(NetRing *ring, NetPacket *packet) {
    if (!ring || !packet) {
        return NET_ERR_INVALID_ARG;
    }

    uint32_t current_tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint32_t current_head = atomic_load_explicit(&ring->head, memory_order_acquire);

    if (current_tail == current_head) {
        return NET_ERR_RING_EMPTY;
    }

    while (true) {
        if (current_tail == current_head) {
            return NET_ERR_RING_EMPTY;
        }

        if (atomic_compare_exchange_weak_explicit(&ring->tail, &current_tail, current_tail + 1,
                                                  memory_order_release, memory_order_relaxed)) {
            break;
        }
        current_head = atomic_load_explicit(&ring->head, memory_order_acquire);
    }

    uint32_t index = current_tail & ring->capacity_mask;
    memcpy(packet, &ring->buffer[index], sizeof(NetPacket));

    return NET_SUCCESS;
}

static inline NetError net_arena_init(NetArena *arena, uint8_t *memory_pool, size_t pool_size) {
    if (!arena || !memory_pool || pool_size == 0) {
        return NET_ERR_INVALID_ARG;
    }
    arena->memory_pool = memory_pool;
    arena->pool_size = pool_size;
    atomic_init(&arena->offset, 0);
    return NET_SUCCESS;
}

static inline void *net_arena_alloc(NetArena *arena, size_t size) {
    if (!arena || size == 0) {
        return NULL;
    }

    const size_t alignment = NET_CACHE_LINE_SIZE;
    size_t current_offset, next_offset;

    do {
        current_offset = atomic_load_explicit(&arena->offset, memory_order_relaxed);
        size_t padding = (alignment - (current_offset % alignment)) % alignment;
        size_t total_size = size + padding;

        if (current_offset + total_size > arena->pool_size) {
            return NULL;
        }
        next_offset = current_offset + total_size;
    } while (!atomic_compare_exchange_weak_explicit(&arena->offset, &current_offset, next_offset,
                                                  memory_order_acq_rel, memory_order_relaxed));

    size_t padding = (alignment - (current_offset % alignment)) % alignment;
    return (void *)(arena->memory_pool + current_offset + padding);
}

static inline void net_arena_reset(NetArena *arena) {
    if (arena) {
        atomic_store_explicit(&arena->offset, 0, memory_order_release);
    }
}

static inline NetError net_flow_tracker_init(NetFlowTracker *tracker, NetFlowEntry *entries, uint32_t capacity) {
    if (!tracker || !entries || !net_is_power_of_two(capacity)) {
        return NET_ERR_INVALID_ARG;
    }
    tracker->entries = entries;
    tracker->capacity = capacity;
    tracker->mask = capacity - 1;
    atomic_init(&tracker->active_flows, 0);
    memset(tracker->entries, 0, capacity * sizeof(NetFlowEntry));
    return NET_SUCCESS;
}

static inline uint32_t net_flow_hash(uint32_t flow_id) {
    flow_id ^= flow_id >> 16;
    flow_id *= 0x85ebca6b;
    flow_id ^= flow_id >> 13;
    flow_id *= 0xc2b2ae35;
    flow_id ^= flow_id >> 16;
    return flow_id;
}

static inline NetError net_flow_tracker_insert(NetFlowTracker *tracker, uint32_t flow_id, uint64_t timestamp_ns) {
    if (!tracker || flow_id == 0) {
        return NET_ERR_INVALID_ARG;
    }

    uint32_t hash = net_flow_hash(flow_id);
    uint32_t index = hash & tracker->mask;
    NetFlowEntry new_entry = {
        .flow_id = flow_id,
        .last_seen_ns = timestamp_ns,
        .packet_count = 1,
        .active = 1,
        .robin_hood_dib = 0
    };

    uint32_t dib = 0;
    for (uint32_t i = 0; i < tracker->capacity; i++) {
        uint32_t probe_index = (index + i) & tracker->mask;
        NetFlowEntry *curr = &tracker->entries[probe_index];

        if (!curr->active) {
            new_entry.robin_hood_dib = dib;
            memcpy(curr, &new_entry, sizeof(NetFlowEntry));
            atomic_fetch_add_explicit(&tracker->active_flows, 1, memory_order_relaxed);
            return NET_SUCCESS;
        }

        if (curr->flow_id == flow_id) {
            curr->last_seen_ns = timestamp_ns;
            curr->packet_count++;
            return NET_SUCCESS;
        }

        if (curr->robin_hood_dib < dib) {
            NetFlowEntry temp = *curr;
            new_entry.robin_hood_dib = dib;
            memcpy(curr, &new_entry, sizeof(NetFlowEntry));
            new_entry = temp;
            dib = curr->robin_hood_dib;
        }

        dib++;
        if (dib >= tracker->capacity) {
            return NET_ERR_ARENA_EXHAUSTED;
        }
    }

    return NET_ERR_RING_FULL;
}

static inline NetError net_flow_tracker_lookup(NetFlowTracker *tracker, uint32_t flow_id, NetFlowEntry *out_entry) {
    if (!tracker || !out_entry || flow_id == 0) {
        return NET_ERR_INVALID_ARG;
    }

    uint32_t hash = net_flow_hash(flow_id);
    uint32_t index = hash & tracker->mask;

    for (uint32_t i = 0; i < tracker->capacity; i++) {
        uint32_t probe_index = (index + i) & tracker->mask;
        const NetFlowEntry *curr = &tracker->entries[probe_index];

        if (!curr->active) {
            return NET_ERR_FLOW_NOT_FOUND;
        }

        if (curr->flow_id == flow_id) {
            memcpy(out_entry, curr, sizeof(NetFlowEntry));
            return NET_SUCCESS;
        }

        if (curr->robin_hood_dib < i) {
            return NET_ERR_FLOW_NOT_FOUND;
        }
    }

    return NET_ERR_FLOW_NOT_FOUND;
}

#endif /* NET_RING_H */