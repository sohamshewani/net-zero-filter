#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <arpa/inet.h>

#define CACHE_LINE_SIZE 64
#define MAX_EVENTS 64
#define BATCH_SIZE 32
#define ARENA_BLOCK_SIZE (2 * 1024 * 1024)
#define FLOW_TABLE_CAPACITY 4096
#define FLOW_TABLE_MASK (FLOW_TABLE_CAPACITY - 1)

typedef enum {
    NET_SUCCESS = 0,
    NET_ERR_INVALID_ARG = -1,
    NET_ERR_RING_FULL = -2,
    NET_ERR_RING_EMPTY = -3,
    NET_ERR_ARENA_EXHAUSTED = -4,
    NET_ERR_FLOW_EXISTS = -5,
    NET_ERR_NOT_FOUND = -6,
    NET_ERR_IO = -7
} NetError;

typedef struct _Alignas(CACHE_LINE_SIZE) {
    uint8_t *data;
    uint32_t length;
    uint32_t capacity;
    uint64_t timestamp;
    uint32_t flow_id;
    uint32_t flags;
} NetPacket;

typedef struct _Alignas(CACHE_LINE_SIZE) {
    _Atomic uint64_t head;
    char pad1[CACHE_LINE_SIZE - sizeof(_Atomic uint64_t)];
    _Atomic uint64_t tail;
    char pad2[CACHE_LINE_SIZE - sizeof(_Atomic uint64_t)];
    uint32_t capacity;
    uint32_t mask;
    NetPacket *buffer;
} NetRing;

typedef struct _Alignas(CACHE_LINE_SIZE) {
    uint8_t *memory_pool;
    size_t total_size;
    _Atomic size_t offset;
    char pad[CACHE_LINE_SIZE - sizeof(size_t) - sizeof(_Atomic size_t) - sizeof(uint8_t*)];
} NetArena;

typedef struct {
    uint32_t flow_id;
    uint32_t src_ip;
    uint32_t dst_ip;
    uint16_t src_port;
    uint16_t dst_port;
    uint8_t protocol;
    uint8_t robin_hood_dib;
    uint64_t last_seen;
    uint64_t packet_count;
    bool active;
} NetFlowEntry;

typedef struct _Alignas(CACHE_LINE_SIZE) {
    NetFlowEntry entries[FLOW_TABLE_CAPACITY];
    _Atomic uint32_t active_flows;
    char pad[CACHE_LINE_SIZE - sizeof(_Atomic uint32_t)];
} NetFlowTracker;

typedef struct {
    int epoll_fd;
    int listen_fd;
    volatile bool running;
    NetArena arena;
    NetRing ring;
    NetFlowTracker flow_tracker;
    uint64_t dropped_packets;
    uint64_t processed_packets;
} NetFilterEngine;

static NetFilterEngine g_engine;

static inline uint32_t calculate_flow_hash(uint32_t src_ip, uint32_t dst_ip, uint16_t src_port, uint16_t dst_port, uint8_t proto) {
    uint32_t hash = src_ip ^ dst_ip;
    hash = ((hash << 13) | (hash >> 19)) ^ src_port;
    hash = ((hash << 17) | (hash >> 15)) ^ dst_port;
    hash = ((hash << 5) | (hash >> 27)) ^ proto;
    return hash;
}

NetError net_ring_init(NetRing *ring, uint32_t capacity) {
    if (!ring || (capacity == 0) || ((capacity & (capacity - 1)) != 0)) {
        return NET_ERR_INVALID_ARG;
    }
    ring->capacity = capacity;
    ring->mask = capacity - 1;
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    
    posix_memalign((void **)&ring->buffer, CACHE_LINE_SIZE, sizeof(NetPacket) * capacity);
    if (!ring->buffer) {
        return NET_ERR_ARENA_EXHAUSTED;
    }
    memset(ring->buffer, 0, sizeof(NetPacket) * capacity);
    return NET_SUCCESS;
}

void net_ring_free(NetRing *ring) {
    if (ring && ring->buffer) {
        free(ring->buffer);
        ring->buffer = NULL;
    }
}

NetError net_ring_enqueue(NetRing *ring, const NetPacket *packet) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);

    if ((tail - head) >= ring->capacity) {
        return NET_ERR_RING_FULL;
    }

    uint64_t idx = tail & ring->mask;
    ring->buffer[idx] = *packet;

    atomic_store_explicit(&ring->tail, tail + 1, memory_order_release);
    return NET_SUCCESS;
}

NetError net_ring_dequeue(NetRing *ring, NetPacket *packet) {
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);

    if (head == tail) {
        return NET_ERR_RING_EMPTY;
    }

    uint64_t idx = head & ring->mask;
    *packet = ring->buffer[idx];

    atomic_store_explicit(&ring->head, head + 1, memory_order_release);
    return NET_SUCCESS;
}

NetError net_arena_init(NetArena *arena, size_t size) {
    if (!arena || size == 0) {
        return NET_ERR_INVALID_ARG;
    }
    arena->total_size = size;
    atomic_init(&arena->offset, 0);
    
    int ret = posix_memalign((void **)&arena->memory_pool, CACHE_LINE_SIZE, size);
    if (ret != 0 || !arena->memory_pool) {
        return NET_ERR_ARENA_EXHAUSTED;
    }
    memset(arena->memory_pool, 0, size);
    return NET_SUCCESS;
}

void net_arena_free(NetArena *arena) {
    if (arena && arena->memory_pool) {
        free(arena->memory_pool);
        arena->memory_pool = NULL;
        arena->total_size = 0;
        atomic_store_explicit(&arena->offset, 0, memory_order_relaxed);
    }
}

void* net_arena_alloc(NetArena *arena, size_t size) {
    size_t aligned_size = (size + (CACHE_LINE_SIZE - 1)) & ~(CACHE_LINE_SIZE - 1);
    size_t current = atomic_load_explicit(&arena->offset, memory_order_relaxed);

    while (1) {
        if (current + aligned_size > arena->total_size) {
            return NULL;
        }
        if (atomic_compare_exchange_weak_explicit(&arena->offset, &current, current + aligned_size,
                                                  memory_order_relaxed, memory_order_relaxed)) {
            return &arena->memory_pool[current];
        }
    }
}

void net_arena_reset(NetArena *arena) {
    atomic_store_explicit(&arena->offset, 0, memory_order_release);
}

void net_flow_tracker_init(NetFlowTracker *tracker) {
    if (!tracker) return;
    memset(tracker->entries, 0, sizeof(tracker->entries));
    atomic_init(&tracker->active_flows, 0);
}

NetError net_flow_tracker_update(NetFlowTracker *tracker, uint32_t src_ip, uint32_t dst_ip, uint16_t src_port, uint16_t dst_port, uint8_t proto) {
    uint32_t flow_id = calculate_flow_hash(src_ip, dst_ip, src_port, dst_port, proto);
    uint32_t idx = flow_id & FLOW_TABLE_MASK;
    uint32_t dib = 0;

    NetFlowEntry new_entry = {
        .flow_id = flow_id,
        .src_ip = src_ip,
        .dst_ip = dst_ip,
        .src_port = src_port,
        .dst_port = dst_port,
        .protocol = proto,
        .robin_hood_dib = 0,
        .last_seen = (uint64_t)time(NULL),
        .packet_count = 1,
        .active = true
    };

    for (uint32_t i = 0; i < FLOW_TABLE_CAPACITY; ++i) {
        uint32_t probe_idx = (idx + i) & FLOW_TABLE_MASK;
        NetFlowEntry *curr = &tracker->entries[probe_idx];

        if (!curr->active) {
            tracker->entries[probe_idx] = new_entry;
            tracker->entries[probe_idx].robin_hood_dib = dib;
            atomic_fetch_add_explicit(&tracker->active_flows, 1, memory_order_relaxed);
            return NET_SUCCESS;
        }

        if (curr->flow_id == flow_id) {
            curr->packet_count++;
            curr->last_seen = (uint64_t)time(NULL);
            return NET_SUCCESS;
        }

        if (curr->robin_hood_dib < dib) {
            NetFlowEntry temp = tracker->entries[probe_idx];
            tracker->entries[probe_idx] = new_entry;
            tracker->entries[probe_idx].robin_hood_dib = dib;
            new_entry = temp;
            dib = temp.robin_hood_dib;
        }

        dib++;
    }

    return NET_ERR_FLOW_EXISTS;
}

void net_flow_tracker_gc(NetFlowTracker *tracker, uint64_t max_idle_seconds) {
    uint64_t now = (uint64_t)time(NULL);
    for (uint32_t i = 0; i < FLOW_TABLE_CAPACITY; ++i) {
        NetFlowEntry *curr = &tracker->entries[i];
        if (curr->active && (now - curr->last_seen > max_idle_seconds)) {
            curr->active = false;
            atomic_fetch_sub_explicit(&tracker->active_flows, 1, memory_order_relaxed);
        }
    }
}

static void handle_signal(int sig) {
    (void)sig;
    g_engine.running = false;
}

NetError net_filter_init(NetFilterEngine *engine, int port) {
    memset(engine, 0, sizeof(NetFilterEngine));
    engine->running = true;

    NetError err = net_arena_init(&engine->arena, ARENA_BLOCK_SIZE);
    if (err != NET_SUCCESS) return err;

    err = net_ring_init(&engine->ring, 1024);
    if (err != NET_SUCCESS) {
        net_arena_free(&engine->arena);
        return err;
    }

    net_flow_tracker_init(&engine->flow_tracker);

    engine->listen_fd = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
    if (engine->listen_fd < 0) {
        net_ring_free(&engine->ring);
        net_arena_free(&engine->arena);
        return NET_ERR_IO;
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
        net_ring_free(&engine->ring);
        net_arena_free(&engine->arena);
        return NET_ERR_IO;
    }

    engine->epoll_fd = epoll_create1(0);
    if (engine->epoll_fd < 0) {
        close(engine->listen_fd);
        net_ring_free(&engine->ring);
        net_arena_free(&engine->arena);
        return NET_ERR_IO;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = engine->listen_fd;
    if (epoll_ctl(engine->epoll_fd, EPOLL_CTL_ADD, engine->listen_fd, &ev) < 0) {
        close(engine->epoll_fd);
        close(engine->listen_fd);
        net_ring_free(&engine->ring);
        net_arena_free(&engine->arena);
        return NET_ERR_IO;
    }

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    return NET_SUCCESS;
}

void net_filter_cleanup(NetFilterEngine *engine) {
    if (engine->epoll_fd >= 0) close(engine->epoll_fd);
    if (engine->listen_fd >= 0) close(engine->listen_fd);
    net_ring_free(&engine->ring);
    net_arena_free(&engine->arena);
}

NetError net_filter_process_packet(NetFilterEngine *engine, uint8_t *data, uint32_t length, struct sockaddr_in *peer) {
    if (length < sizeof(struct iphdr)) {
        engine->dropped_packets++;
        return NET_ERR_INVALID_ARG;
    }

    struct iphdr *ip = (struct iphdr *)data;
    uint32_t src_ip = ip->saddr;
    uint32_t dst_ip = ip->daddr;
    uint8_t proto = ip->protocol;
    uint16_t src_port = 0, dst_port = 0;

    if (proto == IPPROTO_UDP && length >= sizeof(struct iphdr) + sizeof(struct udphdr)) {
        struct udphdr *udp = (struct udphdr *)(data + (ip->ihl * 4));
        src_port = ntohs(udp->source);
        dst_port = ntohs(udp->dest);
    } else if (proto == IPPROTO_TCP && length >= sizeof(struct iphdr) + sizeof(struct tcphdr)) {
        struct tcphdr *tcp = (struct tcphdr *)(data + (ip->ihl * 4));
        src_port = ntohs(tcp->source);
        dst_port = ntohs(tcp->dest);
    }

    NetError err = net_flow_tracker_update(&engine->flow_tracker, src_ip, dst_ip, src_port, dst_port, proto);
    if (err != NET_SUCCESS) {
        engine->dropped_packets++;
        return err;
    }

    void *arena_buf = net_arena_alloc(&engine->arena, length);
    if (!arena_buf) {
        net_arena_reset(&engine->arena);
        net_flow_tracker_gc(&engine->flow_tracker, 30);
        arena_buf = net_arena