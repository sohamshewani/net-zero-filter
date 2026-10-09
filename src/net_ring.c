#include "net_ring.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

bool net_arena_init(NetArena *arena, size_t total_size) {
    if (!arena || total_size == 0) {
        return false;
    }

    size_t aligned_size = (total_size + NET_CACHE_LINE_SIZE - 1) & ~(NET_CACHE_LINE_SIZE - 1);
    
    // Allocate 64-byte aligned memory block
    int ret = posix_memalign((void **)&arena->buffer, NET_CACHE_LINE_SIZE, aligned_size);
    if (ret != 0 || !arena->buffer) {
        arena->buffer = NULL;
        return false;
    }

    arena->capacity = aligned_size;
    atomic_store_explicit(&arena->offset, 0, memory_order_relaxed);
    atomic_store_explicit(&arena->epoch, 0, memory_order_relaxed);
    return true;
}

void net_arena_destroy(NetArena *arena) {
    if (arena && arena->buffer) {
        free(arena->buffer);
        arena->buffer = NULL;
        arena->capacity = 0;
        atomic_store_explicit(&arena->offset, 0, memory_order_relaxed);
        atomic_store_explicit(&arena->epoch, 0, memory_order_relaxed);
    }
}

void *net_arena_alloc(NetArena *arena, size_t size) {
    if (!arena || !arena->buffer || size == 0) {
        return NULL;
    }

    // Align allocation size to 64 bytes
    size_t aligned_alloc_size = (size + NET_CACHE_LINE_SIZE - 1) & ~(NET_CACHE_LINE_SIZE - 1);
    
    size_t current_offset = atomic_load_explicit(&arena->offset, memory_order_relaxed);
    
    while (1) {
        if (current_offset + aligned_alloc_size > arena->capacity) {
            return NULL; // Arena exhaustion
        }
        
        if (atomic_compare_exchange_weak_explicit(&arena->offset,
                                                  &current_offset,
                                                  current_offset + aligned_alloc_size,
                                                  memory_order_acq_rel,
                                                  memory_order_relaxed)) {
            return arena->buffer + current_offset;
        }
    }
}

void net_arena_reset(NetArena *arena) {
    if (!arena) {
        return;
    }
    atomic_store_explicit(&arena->offset, 0, memory_order_release);
    atomic_fetch_add_explicit(&arena->epoch, 1, memory_order_relaxed);
}

bool net_ring_init(NetRing *ring, size_t capacity) {
    if (!ring) {
        return false;
    }

    // Enforce strict power of two capacity
    if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
        return false;
    }

    ring->slots = (NetRingSlot *)calloc(capacity, sizeof(NetRingSlot));
    if (!ring->slots) {
        return false;
    }

    ring->capacity = capacity;
    ring->mask = capacity - 1;

    atomic_store_explicit(&ring->head, 0, memory_order_relaxed);
    atomic_store_explicit(&ring->tail, 0, memory_order_relaxed);

    for (size_t i = 0; i < capacity; ++i) {
        atomic_store_explicit(&ring->slots[i].sequence, i, memory_order_relaxed);
        ring->slots[i].packet = NULL;
    }

    return true;
}

void net_ring_destroy(NetRing *ring) {
    if (ring) {
        if (ring->slots) {
            free(ring->slots);
            ring->slots = NULL;
        }
        ring->capacity = 0;
        ring->mask = 0;
        atomic_store_explicit(&ring->head, 0, memory_order_relaxed);
        atomic_store_explicit(&ring->tail, 0, memory_order_relaxed);
    }
}

bool net_ring_enqueue(NetRing *ring, NetPacket *packet) {
    if (!ring || !packet) {
        return false;
    }

    NetRingSlot *slot;
    size_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);

    while (1) {
        slot = &ring->slots[head & ring->mask];
        size_t seq = atomic_load_explicit(&slot->sequence, memory_order_acquire);
        intptr_t dif = (intptr_t)seq - (intptr_t)head;

        if (dif == 0) {
            if (atomic_compare_exchange_weak_explicit(&ring->head,
                                                      &head,
                                                      head + 1,
                                                      memory_order_relaxed,
                                                      memory_order_relaxed)) {
                break;
            }
        } else if (dif < 0) {
            // Ring buffer full
            return false;
        } else {
            head = atomic_load_explicit(&ring->head, memory_order_relaxed);
        }
    }

    slot->packet = packet;
    atomic_store_explicit(&slot->sequence, head + 1, memory_order_release);
    return true;
}

bool net_ring_dequeue(NetRing *ring, NetPacket **packet) {
    if (!ring || !packet) {
        return false;
    }

    NetRingSlot *slot;
    size_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);

    while (1) {
        slot = &ring->slots[tail & ring->mask];
        size_t seq = atomic_load_explicit(&slot->sequence, memory_order_acquire);
        intptr_t dif = (intptr_t)seq - (intptr_t)(tail + 1);

        if (dif == 0) {
            if (atomic_compare_exchange_weak_explicit(&ring->tail,
                                                      &tail,
                                                      tail + 1,
                                                      memory_order_relaxed,
                                                      memory_order_relaxed)) {
                break;
            }
        } else if (dif < 0) {
            // Ring buffer empty
            return false;
        } else {
            tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
        }
    }

    *packet = slot->packet;
    slot->packet = NULL;
    atomic_store_explicit(&slot->sequence, tail + ring->capacity, memory_order_release);
    return true;
}

size_t net_ring_size(const NetRing *ring) {
    if (!ring) {
        return 0;
    }
    size_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    size_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    if (head >= tail) {
        return head - tail;
    }
    return (ring->capacity - tail) + head;
}