#include "net_arena.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

bool net_arena_init(NetArena *arena, size_t total_size) {
    if (!arena || total_size == 0) {
        return false;
    }

    size_t aligned_size = (total_size + NET_ARENA_ALIGNMENT - 1) & ~(NET_ARENA_ALIGNMENT - 1);
    
    int ret = posix_memalign((void **)&arena->base, NET_ARENA_ALIGNMENT, aligned_size);
    if (ret != 0 || !arena->base) {
        arena->base = NULL;
        return false;
    }

    arena->capacity = aligned_size;
    atomic_init(&arena->offset, 0);
    atomic_init(&arena->epoch, 1);
    atomic_init(&arena->active_allocations, 0);

    return true;
}

void net_arena_destroy(NetArena *arena) {
    if (!arena) {
        return;
    }

    if (arena->base) {
        free(arena->base);
        arena->base = NULL;
    }

    arena->capacity = 0;
    atomic_store_explicit(&arena->offset, 0, memory_order_relaxed);
    atomic_store_explicit(&arena->epoch, 0, memory_order_relaxed);
    atomic_store_explicit(&arena->active_allocations, 0, memory_order_relaxed);
}

void *net_arena_alloc(NetArena *arena, size_t size) {
    if (!arena || size == 0) {
        return NULL;
    }

    size_t aligned_size = (size + NET_ARENA_ALIGNMENT - 1) & ~(NET_ARENA_ALIGNMENT - 1);
    if (aligned_size > arena->capacity) {
        return NULL;
    }

    size_t current_offset = atomic_load_explicit(&arena->offset, memory_order_relaxed);
    
    while (1) {
        if (current_offset + aligned_size > arena->capacity) {
            return NULL; 
        }

        if (atomic_compare_exchange_weak_explicit(
                &arena->offset,
                &current_offset,
                current_offset + aligned_size,
                memory_order_acq_rel,
                memory_order_relaxed)) {
            
            atomic_fetch_add_explicit(&arena->active_allocations, 1, memory_order_relaxed);
            return (void *)(arena->base + current_offset);
        }
    }
}

void net_arena_reset(NetArena *arena) {
    if (!arena) {
        return;
    }

    atomic_store_explicit(&arena->offset, 0, memory_order_release);
    atomic_fetch_add_explicit(&arena->epoch, 1, memory_order_acq_rel);
    atomic_store_explicit(&arena->active_allocations, 0, memory_order_release);
}

bool net_arena_owns(const NetArena *arena, const void *ptr) {
    if (!arena || !ptr || !arena->base) {
        return false;
    }

    const uint8_t *p = (const uint8_t *)ptr;
    const uint8_t *start = arena->base;
    const uint8_t *end = arena->base + arena->capacity;

    return (p >= start && p < end);
}

size_t net_arena_available(const NetArena *arena) {
    if (!arena) {
        return 0;
    }

    size_t current_offset = atomic_load_explicit(&arena->offset, memory_order_relaxed);
    if (current_offset >= arena->capacity) {
        return 0;
    }

    return arena->capacity - current_offset;
}