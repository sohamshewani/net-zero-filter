#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdatomic.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <signal.h>
#include <immintrin.h>

#define CACHE_LINE_SIZE 64
#define NET_RING_DEFAULT_CAPACITY 4096
#define NET_ARENA_SIZE (1024 * 1024 * 64) // 64 MB
#define NET_FLOW_TABLE_SIZE 8192
#define MAX_EVENTS 64
#define BATCH_SIZE 32

typedef struct {
    uint32_t s_addr;
    uint32_t d_addr;
    uint16_t s_port;
    uint16_t d_port;
    uint8_t  proto;
} __attribute__((aligned(8))) NetFlowKey;

typedef struct {
    NetFlowKey key;
    uint64_t packet_count;
    uint64_t byte_count;
    uint64_t last_seen_epoch_ns;
    uint32_t robin_hood_dib;
    bool active;
} __attribute__((aligned(64))) NetFlowEntry;

_Alignas(CACHE_LINE_SIZE) typedef struct {
    uint8_t *buffer;
    size_t capacity;
    _Atomic size_t offset;
} NetArena;

typedef struct {
    uint8_t *data;
    uint32_t length;
    uint32_t capacity;
    uint64_t timestamp_ns;
    NetFlowKey flow_id;
    bool zero_copy;
} __attribute__((aligned(64))) NetPacket;

_Alignas(CACHE_LINE_SIZE) typedef struct {
    NetPacket *slots;
    uint32_t capacity;
    uint32_t mask;
    _Alignas(CACHE_LINE_SIZE) _Atomic uint64_t head;
    _Alignas(CACHE_LINE_SIZE) _Atomic uint64_t tail;
} NetRing;

_Alignas(CACHE_LINE_SIZE) typedef struct {
    NetFlowEntry entries[NET_FLOW_TABLE_SIZE];
    _Atomic size_t active_flows;
} NetFlowTracker;

typedef struct {
    int epoll_fd;
    int listen_fd;
    NetRing ring;
    NetArena arena;
    NetFlowTracker flow_tracker;
    _Atomic bool running;
} NetEngine;

static inline uint64_t net_current_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline uint32_t net_hash_flow(const NetFlowKey *key) {
    uint32_t hash = 2166136261U;
    const uint8_t *ptr = (const uint8_t *)key;
    for (size_t i = 0; i < sizeof(NetFlowKey); i++) {
        hash ^= ptr[i];
        hash *= 16777619U;
    }
    return hash;
}

bool net_arena_init(NetArena *arena, size_t capacity) {
    arena->capacity = capacity;
    atomic_store_explicit(&arena->offset, 0, memory_order_relaxed);
    int ret = posix_memalign((void **)&arena->buffer, CACHE_LINE_SIZE, capacity);
    if (ret != 0 || arena->buffer == NULL) {
        return false;
    }
    memset(arena->buffer, 0, capacity);
    return true;
}

void net_arena_free(NetArena *arena) {
    if (arena->buffer) {
        free(arena->buffer);
        arena->buffer = NULL;
    }
    arena->capacity = 0;
    atomic_store_explicit(&arena->offset, 0, memory_order_relaxed);
}

void *net_arena_alloc(NetArena *arena, size_t size) {
    size_t aligned_size = (size + (CACHE_LINE_SIZE - 1)) & ~(CACHE_LINE_SIZE - 1);
    size_t current = atomic_load_explicit(&arena->offset, memory_order_relaxed);
    
    while (1) {
        if (current + aligned_size > arena->capacity) {
            return NULL; // Arena exhaustion
        }
        if (atomic_compare_exchange_weak_explicit(&arena->offset, &current, current + aligned_size,
                                                  memory_order_release, memory_order_relaxed)) {
            return (void *)(arena->buffer + current);
        }
    }
}

void net_arena_reset(NetArena *arena) {
    atomic_store_explicit(&arena->offset, 0, memory_order_release);
}

bool net_ring_init(NetRing *ring, uint32_t capacity) {
    if ((capacity & (capacity - 1)) != 0) {
        return false; // Must be power of two
    }
    ring->capacity = capacity;
    ring->mask = capacity - 1;
    atomic_store_explicit(&ring->head, 0, memory_order_relaxed);
    atomic_store_explicit(&ring->tail, 0, memory_order_relaxed);
    
    int ret = posix_memalign((void **)&ring->slots, CACHE_LINE_SIZE, sizeof(NetPacket) * capacity);
    if (ret != 0 || ring->slots == NULL) {
        return false;
    }
    memset(ring->slots, 0, sizeof(NetPacket) * capacity);
    return true;
}

void net_ring_free(NetRing *ring) {
    if (ring->slots) {
        free(ring->slots);
        ring->slots = NULL;
    }
    ring->capacity = 0;
    ring->mask = 0;
}

bool net_ring_enqueue(NetRing *ring, const NetPacket *packet) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);

    if ((tail - head) >= ring->capacity) {
        return false; // Ring full (Drop-tail policy)
    }

    ring->slots[tail & ring->mask] = *packet;
    atomic_store_explicit(&ring->tail, tail + 1, memory_order_release);
    return true;
}

bool net_ring_dequeue(NetRing *ring, NetPacket *packet) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);

    if (head == tail) {
        return false; // Ring empty
    }

    *packet = ring->slots[head & ring->mask];
    atomic_store_explicit(&ring->head, head + 1, memory_order_release);
    return true;
}

void net_flow_tracker_init(NetFlowTracker *tracker) {
    memset(tracker->entries, 0, sizeof(tracker->entries));
    atomic_store_explicit(&tracker->active_flows, 0, memory_order_relaxed);
}

bool net_flow_tracker_update(NetFlowTracker *tracker, const NetFlowKey *key, uint32_t packet_len, uint64_t timestamp_ns) {
    uint32_t hash = net_hash_flow(key);
    uint32_t idx = hash % NET_FLOW_TABLE_SIZE;
    uint32_t dib = 0;

    NetFlowEntry new_entry;
    memset(&new_entry, 0, sizeof(new_entry));
    new_entry.key = *key;
    new_entry.packet_count = 1;
    new_entry.byte_count = packet_len;
    new_entry.last_seen_epoch_ns = timestamp_ns;
    new_entry.robin_hood_dib = 0;
    new_entry.active = true;

    while (true) {
        NetFlowEntry *curr = &tracker->entries[idx];

        if (!curr->active) {
            *curr = new_entry;
            curr->robin_hood_dib = dib;
            atomic_fetch_add_explicit(&tracker->active_flows, 1, memory_order_relaxed);
            return true;
        }

        if (memcmp(&curr->key, key, sizeof(NetFlowKey)) == 0) {
            curr->packet_count++;
            curr->byte_count += packet_len;
            curr->last_seen_epoch_ns = timestamp_ns;
            return true;
        }

        if (curr->robin_hood_dib < dib) {
            NetFlowEntry temp = *curr;
            *curr = new_entry;
            curr->robin_hood_dib = dib;
            new_entry = temp;
            dib = new_entry.robin_hood_dib;
        }

        dib++;
        idx = (idx + 1) % NET_FLOW_TABLE_SIZE;
        if (dib >= NET_FLOW_TABLE_SIZE) {
            return false; // Table exhausted
        }
    }
}

void net_flow_tracker_gc(NetFlowTracker *tracker, uint64_t current_time_ns, uint64_t timeout_ns) {
    for (size_t i = 0; i < NET_FLOW_TABLE_SIZE; i++) {
        NetFlowEntry *curr = &tracker->entries[i];
        if (curr->active && (current_time_ns - curr->last_seen_epoch_ns > timeout_ns)) {
            curr->active = false;
            memset(&curr->key, 0, sizeof(NetFlowKey));
            curr->packet_count = 0;
            curr->byte_count = 0;
            curr->last_seen_epoch_ns = 0;
            curr->robin_hood_dib = 0;
            size_t active = atomic_load_explicit(&tracker->active_flows, memory_order_relaxed);
            if (active > 0) {
                atomic_fetch_sub_explicit(&tracker->active_flows, 1, memory_order_relaxed);
            }
        }
    }
}

static volatile sig_atomic_t g_shutdown_requested = 0;

static void net_signal_handler(int signum) {
    (void)signum;
    g_shutdown_requested = 1;
}

bool net_engine_init(NetEngine *engine, int port) {
    atomic_store_explicit(&engine->running, true, memory_order_relaxed);
    
    if (!net_ring_init(&engine->ring, NET_RING_DEFAULT_CAPACITY)) {
        return false;
    }
    
    if (!net_arena_init(&engine->arena, NET_ARENA_SIZE)) {
        net_ring_free(&engine->ring);
        return false;
    }
    
    net_flow_tracker_init(&engine->flow_tracker);

    engine->listen_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (engine->listen_fd < 0) {
        net_arena_free(&engine->arena);
        net_ring_free(&engine->ring);
        return false;
    }

    int opt = 1;
    setsockopt(engine->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(engine->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(engine->listen_fd);
        net_arena_free(&engine->arena);
        net_ring_free(&engine->ring);
        return false;
    }

    int flags = fcntl(engine->listen_fd, F_GETFL, 0);
    fcntl(engine->listen_fd, F_SETFL, flags | O_NONBLOCK);

    engine->epoll_fd = epoll_create1(0);
    if (engine->epoll_fd < 0) {
        close(engine->listen_fd);
        net_arena_free(&engine->arena);
        net_ring_free(&engine->ring);
        return false;
    }

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = engine->listen_fd;

    if (epoll_ctl(engine->epoll_fd, EPOLL_CTL_ADD, engine->listen_fd, &ev) < 0) {
        close(engine->epoll_fd);
        close(engine->listen_fd);
        net_arena_free(&engine->arena);
        net_ring_free(&engine->ring);
        return false;
    }

    struct sigaction sa;
    sa.handler = net_signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    return true;
}

void net_engine_shutdown(NetEngine *engine) {
    atomic_store_explicit(&engine->running, false, memory_order_release);
    if (engine->epoll_fd >= 0) {
        close(engine->epoll_fd);
    }
    if (engine->listen_fd >= 0) {
        close(engine->listen_fd);
    }
    net_arena_free(&engine->arena);
    net_ring_free(&engine->ring);
}

void net_engine_run(NetEngine *engine) {
    struct epoll_event events[MAX_EVENTS];
    struct mmsghdr msgs[BATCH_SIZE];
    struct iovec iovecs[BATCH_SIZE];
    uint8_t *arena_blocks[BATCH_SIZE];

    while (atomic_load_explicit(&engine->running, memory_order_relaxed) && !g_shutdown_requested) {
        int nready = epoll_wait(engine->epoll_fd, events, MAX_EVENTS, 100);
        if (nready < 0) {
            if (errno == EINTR) continue;
            break;
        }

        uint64_t now = net_current_time_ns();
        net_flow_tracker_gc(&engine->flow_tracker, now, 5000000000ULL); // 5 seconds timeout

        for (int i = 0; i < nready; i++) {
            if (events[i].events & EPOLLIN) {
                while (true) {
                    memset(msgs, 0, sizeof(msgs));
                    memset(iovecs, 0, sizeof(iovecs));

                    for (int j = 0; j < BATCH_SIZE; j++) {
                        void *mem = net_arena_alloc(&engine->arena, 1500);
                        if (!mem) {
                            // Arena exhaustion recovery: Emergency reset and garbage collection
                            net_arena_reset(&engine->arena);
                            mem = net_arena_alloc(&engine->arena, 1500);
                            if (!mem) {
                                break;
                            }
                        }
                        arena_blocks[j] = (uint8_t *)mem;
                        iovecs[j].iov_base = arena_blocks[j];
                        iovecs[j].iov_len = 1500;
                        msgs[j].msg_hdr.msg_iov = &iovecs[j];
                        msgs[j].msg_hdr.msg_iovlen = 1;
                    }

                    int retval = recvmmsg(engine->listen