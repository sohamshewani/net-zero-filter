# net-zero-filter
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/net-zero-filter/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/net-zero-filter/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/net-zero-filter) [![Stars](https://img.shields.io/github/stars/sohamshewani/net-zero-filter?style=flat)](https://github.com/sohamshewani/net-zero-filter)

## Executive Summary & Problem Space

`net-zero-filter` is a high-throughput, ultra-low-latency packet inspection and flow-filtering engine written in modern C (C11). Engineered specifically for kernel-bypass and high-frequency network environments (e.g., DPDK, XDP pipelines), it addresses the fundamental performance bottlenecks of traditional packet processing architectures: dynamic memory allocation overhead, lock contention across multi-core ingress rings, and cache-line bouncing during stateful flow tracking.

Modern high-speed interfaces easily saturate standard system interconnects when packets incur per-allocation overheads (`malloc`/`free`) or require mutual exclusion locks (`pthread_mutex`) on hot paths. `net-zero-filter` eradicates these bottlenecks by employing a deterministic **Zero-Allocation Arena Allocator** (`src/net_arena.c`), a lockless **Multi-Producer Multi-Consumer (MPMC) Ring Buffer** (`src/net_ring.c`), and an isolated flow-tracking state machine (`src/net_flow.c`). By enforcing strict memory alignment and cache-line isolation, the engine achieves deterministic wire-speed filtering suitable for 100GbE+ fabrics.

---

## Structural ASCII Architecture / Dataflow Diagram

The system operates as a pipelined, event-driven engine divided into memory management, concurrency transport, flow state classification, and rule execution layers.

```
 +-----------------------------------------------------------------------+
 |                        Inbound Packet Stream                          |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                     NET RING (`src/net_ring.c`)                       |
 |  [Producer Threads] ---> (Lockless CAS Ring) ---> [Consumer Threads]  |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                     NET ARENA (`src/net_arena.c`)                     |
 |       [Pre-allocated Monotonic Blocks / Zero Dynamic Allocation]      |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                     NET FLOW (`src/net_flow.c`)                       |
 |        [5-Tuple Hash Table & Stateful Connection Tracking]            |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                    NET FILTER (`src/net_filter.c`)                    |
 |          [Bitmask Evaluation & Drop/Forward/Redirect Verdict]         |
 +-----------------------------------------------------------------------+
                                    |
            +-----------------------+-----------------------+
            |                                               |
            v                                               v
    [DROP / TERMINATE]                              [FORWARD / INGEST]
```

---

## Concurrency Model & Memory Invariants

`net-zero-filter` relies on a strict thread-safety and memory ownership model designed to scale linearly across CPU sockets without cache thrashing.

* **Lockless Synchronization (`src/net_ring.c`):** Ring buffers utilize atomic Compare-And-Swap (CAS) operations (`stdatomic.h`) with explicit acquire-release memory ordering. This eliminates kernel transitions and OS scheduler waits on critical paths.
* **Cache-Line Alignment:** Shared control structures are padded to 64-byte boundaries using `_Alignas(64)` to prevent false sharing across distinct core execution units.
* **Arena Monotonic Lifetime (`src/net_arena.c`):** Memory allocations are performed via a pre-initialized arena block. Individual frees are disallowed during packet processing; instead, the entire arena state is reset atomically per processing epoch. This completely eliminates internal fragmentation and metadata overhead.
* **Flow State Isolation (`src/net_flow.c`):** Concurrent state lookups are governed by read-copy-update (RCU)-style epoch markers, ensuring writers do not block high-frequency readers traversing active connection 5-tuples.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation | Path | Time Complexity | Space Complexity | Latency Estimate (P99) |
| :--- | :--- | :--- | :--- | :--- |
| **Ring Enqueue / Dequeue** | Fast Path | $\mathcal{O}(1)$ | $\mathcal{O}(N)$ bounded | $< 15$ ns |
| **Arena Allocation** | Fast Path | $\mathcal{O}(1)$ | $\mathcal{O}(M)$ fixed | $< 5$ ns |
| **Flow Table Lookup** | Fast Path | $\mathcal{O}(1)$ avg, $\mathcal{O}(K)$ worst | $\mathcal{O}(F)$ | $< 45$ ns |
| **Filter Rule Match** | Fast Path | $\mathcal{O}(R)$ ($R = \text{rules}$) | $\mathcal{O}(R)$ | $< 30$ ns |
| **Flow Table Cleanup** | Maintenance | $\mathcal{O}(F)$ ($F = \text{flows}$) | $\mathcal{O}(1)$ | $< 1.2\,\mu\text{s}$ |

---

## Security Architecture & Threat Model

Designed for deployment at the network edge, `net-zero-filter` treats all incoming packet payloads as untrusted and potentially malformed.

* **Input Boundary Validation:** Every packet header is checked against structural bounds before byte-offset parsing (`include/net_types.h`). Truncated or malformed frames are dropped immediately to prevent out-of-bounds reads.
* **Memory Corruption Protections:** The engine avoids unsafe string manipulation APIs and dynamic pointer arithmetic. All state buffers are managed within explicit bounds enforced by compile-time assertions and arena ceiling checks.
* **Denial of Service (DoS) Mitigation:** Flow tracking tables enforce strict capacity limits with bounded collision chains and LRU-based eviction. This prevents hash-collision attacks from exhausting system memory.
* **Hardened Compilation Flags:** Production builds enforce stack protection, read-only relocations (`RELRO`), immediate binding, and position-independent executables (`PIE`).

---

## Build, Verification & Benchmarking Recipes

Ensure your environment includes a C11-compliant compiler (`gcc` or `clang`), `make`, and standard POSIX development utilities.

### 1. Build the Project
Compile the static library, test suite, and benchmarking harness in optimized release mode:
```bash
make clean && make release
```

### 2. Run Verification & Invariant Tests
Execute the automated test suite to validate memory invariants, ring buffer boundaries, and state transition correctness:
```bash
make test
```

### 3. Execute Performance Benchmarks
Run the micro-benchmarking suite (`benchmarks/bench_engine.c`) to measure throughput, ring contention scaling, and per-packet processing latency:
```bash
make bench