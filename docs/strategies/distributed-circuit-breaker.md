# Distributed circuit breaker

This document describes the **distributed circuit breaker** strategy in `Polly.Core`: a multi-instance coordination layer that opens (and recovers) a shared circuit so that when **one node** detects a bad downstream, **all nodes** fail fast together.

It is **additive**. The classic in-process `AddCircuitBreaker` API and behavior are unchanged.

---

## Motivation

| Single-node CB | Multi-instance problem |
|----------------|------------------------|
| Opens only in the process that saw failures | Other replicas keep hammering the dependency |
| Local half-open probe | Many replicas probe at once after break duration |
| Local clocks only | Skew can cause early/late half-open transitions |
| No shared health | Global failure rate is invisible |

The distributed strategy shares **state** and **health contributions** through an `IDistributedCircuitStateStore` (Redis, etcd, SQL, etc.—or the in-memory store for tests).

### Trust model (security)

The state store is a **privileged control plane**. Every writer is treated as a co-equal service instance:

| Capability of a store writer | Impact |
|------------------------------|--------|
| Write circuit snapshot | Open/close the circuit for the whole cluster |
| Publish health as any `InstanceId` | Inflate failure rate or consecutive streaks; trip the cluster |
| Spoof half-open lease owner | Steal the only probe slot |

**Requirements for production:**

1. Mutual authentication and network isolation of the backend (Redis ACL, mTLS, private network).
2. Unique, non-guessable `InstanceId` per process (default GUID is fine).
3. Namespace `CircuitKey` per environment/tenant (`prod:payments-api`, not bare `payments`).
4. Never expose the store to untrusted clients or multi-tenant apps that share one backend without isolation.
5. Prefer `FailClosed` partition policy when the store itself may be attacked or unreliable.

`InMemoryDistributedCircuitStateStore` is for **tests and single-process demos only**. It bounds distinct keys via `MaxTrackedCircuitKeys` (default 10_000).

---

## Architecture

```
┌────────────┐   ┌────────────┐   ┌────────────┐
│  Instance A│   │  Instance B│   │  Instance C│
│ AddDistributedCircuitBreaker │
└─────┬──────┘   └─────┬──────┘   └─────┬──────┘
      │ publish health │ CAS state      │
      └────────────────┼────────────────┘
                       ▼
         ┌─────────────────────────────┐
         │ IDistributedCircuitStateStore│
         │  - DistributedCircuitSnapshot│
         │  - Health contributions      │
         └─────────────────────────────┘
                       ▲
         InMemory (tests) / Redis / custom
```

### Pipeline placement (v8)

Same as other strategies: options → `AddDistributedCircuitBreaker` → internal `DistributedCircuitBreakerResilienceStrategy<T>` → `PipelineComponent` chain.

```
ResiliencePipelineBuilder
  .AddDistributedCircuitBreaker(options)
  .Build()
```

### Components

| Type | Role |
|------|------|
| `IDistributedCircuitStateStore` | Shared read/CAS update of circuit snapshot; health publish/aggregate |
| `InMemoryDistributedCircuitStateStore` | Process-local implementation for tests / co-located demos |
| `DistributedCircuitSnapshot` | Versioned state: `Closed` / `Open` / `HalfOpen` / `Isolated`, `OpenUntilUtc`, half-open lease |
| `DistributedHealthContribution` | Per-instance success/failure/consecutive counts |
| `DistributedHealthAggregate` | Summed cluster throughput, failure rate, max consecutive failures |
| `DistributedCircuitBreakerStrategyOptions` | Circuit key, store, thresholds, skew, partition policy |
| `DistributedCircuitBreakerResilienceStrategy` | Pre-execute gate + post-execute health + CAS transitions |
| `DistributedCircuitPartitionPolicy` | Behavior when the store is unreachable |

### State machine (cluster-wide)

```
        failures (aggregated)
   Closed ──────────────────► Open
     ▲                          │
     │ success probe            │ break elapsed (+ skew)
     │                          ▼
     └──────── HalfOpen ◄───────┘
              (single lease owner)
                 │
                 │ failure probe
                 └──────────────► Open
```

- **Open for all nodes**: any instance that successfully CAS-writes `Open` updates the shared snapshot; others refresh and reject with `BrokenCircuitException`.
- **Half-open race**: exactly one instance may CAS from `Open` → `HalfOpen` with `HalfOpenLeaseOwner = instanceId` and a lease expiry. Others keep rejecting until the owner closes or the lease is abandoned (soft re-open).
- **Single-flight probe**: the lease owner allows **at most one concurrent probe** on that instance (matches classic CB half-open). Additional concurrent calls while the probe is in flight are rejected.
- **Close**: only the lease owner closes on a successful probe (`HalfOpen` → `Closed`).
- **Abandoned lease**: expired half-open leases re-open with `OpenUntilUtc = now - AllowedClockSkew` so the circuit is **immediately eligible** for a new half-open attempt (no full break restart).

---

## Cross-cutting concerns

### 1. Network partition (instance ↔ store)

| Policy | Behavior when `GetAsync` fails |
|--------|--------------------------------|
| `PreferLastKnownState` (default) | Use last successful snapshot (or Closed if none) |
| `FailClosed` | Synthesize Open — protect the dependency |
| `FailOpen` | Treat as Closed — favor caller availability |

Health evaluation **does not open** the circuit if aggregation itself fails (avoids false trips on partial data). Open/CAS failures are retried up to `MaxCasAttempts` or abandoned safely.

### 2. Clock synchronization

- Store **absolute UTC** `OpenUntilUtc` (opened_at + break duration on the writer).
- Readers apply `AllowedClockSkew` **only to extend** the open window:
  - Still open while `now < OpenUntilUtc + AllowedClockSkew` (prevents early probes on lagging clocks).
  - Skew never shortens the break or allows early half-open.
- Prefer NTP / cloud time sync in production; skew is a safety margin, not a full time-sync protocol.

### 3. Half-open races

- Optimistic concurrency via **monotonic `Version`** on every write (`next.Version == expected.Version + 1`).
- Half-open **lease** binds the probe to one `InstanceId` with `HalfOpenLeaseExpiresUtc`.
- **Single-flight** on the owner instance: one in-flight probe at a time.
- Expired lease → soft CAS back to `Open` (short/open-until-past skew) so another instance can probe without a full break restart.

### 4. State refresh / stale windows

| Last known state | Refresh behavior |
|------------------|------------------|
| `Closed` | **Force-refresh** on each execution (minimize fail-open after a remote open) |
| `Open` / `HalfOpen` / other | Throttled by `StateRefreshInterval` (default 100ms) |

Set `StateRefreshInterval = TimeSpan.Zero` for strongest consistency on all paths (higher store load).

### 5. Health metrics aggregation

Each instance keeps a **local sampling window** and publishes:

```text
(instanceId, successCount, failureCount, consecutiveFailures, reportedAtUtc)
```

`GetAggregatedHealthAsync` sums contributions newer than `SamplingDuration`, drops stale ones, and computes:

- `Throughput`, `FailureRate`
- `MaxConsecutiveFailures` (max streak among instances)

**Trip when:**

```text
(Throughput >= MinimumThroughput && FailureRate >= FailureRatio)
  || (ConsecutiveFailureThreshold is set
      && MaxConsecutiveFailures >= threshold
      && Throughput >= min(MinimumThroughput, threshold))
```

Consecutive trips require recent sample volume so a poisoned consecutive counter without traffic cannot open the cluster.

---

## Usage

### Minimal (tests / single process multi-pipeline)

```csharp
using Polly;
using Polly.CircuitBreaker.Distributed;

var store = new InMemoryDistributedCircuitStateStore();

ResiliencePipeline CreateNode(string instanceId) =>
    new ResiliencePipelineBuilder()
        .AddDistributedCircuitBreaker(new DistributedCircuitBreakerStrategyOptions
        {
            CircuitKey = "payments-api",
            InstanceId = instanceId,
            StateStore = store,
            FailureRatio = 0.5,
            MinimumThroughput = 20,
            BreakDuration = TimeSpan.FromSeconds(30),
            AllowedClockSkew = TimeSpan.FromSeconds(2),
            PartitionPolicy = DistributedCircuitPartitionPolicy.PreferLastKnownState,
            ShouldHandle = new PredicateBuilder().Handle<HttpRequestException>(),
        })
        .Build();

var nodeA = CreateNode("node-a");
var nodeB = CreateNode("node-b");
```

### Production sketch (custom store)

Implement `IDistributedCircuitStateStore` with Redis (e.g. `GET`/`WATCH`+`MULTI`/`EVAL` CAS on a JSON blob + hash of health contributions), then:

```csharp
services.AddSingleton<IDistributedCircuitStateStore, RedisDistributedCircuitStateStore>();

services.AddResiliencePipeline("payments", (builder, context) =>
{
    builder.AddDistributedCircuitBreaker(new DistributedCircuitBreakerStrategyOptions
    {
        CircuitKey = "payments-api",
        InstanceId = Environment.MachineName,
        StateStore = context.ServiceProvider.GetRequiredService<IDistributedCircuitStateStore>(),
        // ...
    });
});
```

### Composition tip

```csharp
new ResiliencePipelineBuilder()
    .AddRetry(...)
    .AddDistributedCircuitBreaker(...)  // fail fast cluster-wide
    .AddTimeout(...)
    .Build();
```

---

## Options reference

| Option | Default | Notes |
|--------|---------|-------|
| `CircuitKey` | required | Shared logical name of the dependency |
| `InstanceId` | new GUID if empty | Lease + health identity |
| `StateStore` | required | Coordination backend |
| `BreakDuration` | 5s | Open duration after trip |
| `SamplingDuration` | 30s | Health contribution freshness |
| `FailureRatio` | 0.5 | Cluster failure rate threshold |
| `MinimumThroughput` | 20 | Min samples before ratio trips |
| `ConsecutiveFailureThreshold` | null | Optional max consecutive; also needs throughput ≥ min(MinimumThroughput, threshold) |
| `AllowedClockSkew` | 2s | Extends open window only (never early probe) |
| `HalfOpenLeaseDuration` | 5s | Probe ownership TTL; owner single-flight probe |
| `StateRefreshInterval` | 100ms | Throttle when not Closed; Closed always force-refreshes |
| `PartitionPolicy` | PreferLastKnownState | Store outage behavior (`FailOpen` = availability over protection) |
| `MaxCasAttempts` | 8 | CAS retry budget |
| `ShouldHandle` / `OnOpened` / `OnClosed` / `OnHalfOpened` | standard CB semantics | |
| `StateProvider` | null | Local view of last known state |

---

## Backward compatibility

| Guarantee | How |
|-----------|-----|
| Existing `AddCircuitBreaker` unchanged | Separate types under `Polly.CircuitBreaker.Distributed` |
| No change to existing public CB options surface for classic CB | New options type only |
| Opt-in | Must call `AddDistributedCircuitBreaker` and supply a store |
| Tests cover classic CB still trips locally | `ExistingLocalCircuitBreaker_Unaffected_BackwardCompat` |

---

## Testing

```bash
cd Polly
export PATH="$HOME/.dotnet:$PATH" DOTNET_ROOT="$HOME/.dotnet"

dotnet test test/Polly.Core.Tests/Polly.Core.Tests.csproj -f net10.0 \
  -p:CollectCoverage=false \
  --filter "FullyQualifiedName~Distributed"
```

Coverage includes:

- CAS / version conflicts on the in-memory store
- Multi-node open propagation
- Half-open single-lease acquisition
- Clock skew open window
- Partition policies (prefer last known / fail open / fail closed)
- Multi-instance health aggregation
- Classic CB regression smoke test

---

## Future work

1. **Redis / etcd reference store** in a separate package (`Polly.Distributed` or samples) with production guidance.
2. **Isolated state** propagation (manual isolate/reset across the cluster).
3. **Push notifications** (pub/sub) to invalidate local snapshot cache instead of refresh interval polling.
4. **Quorum open**: require N instances to report unhealthy before opening (reduces single-node false trips).
5. **Slow-call dimension** aligned with multi-dimension local CB.
6. **OpenTelemetry** attributes for lease owner, version, partition mode.
7. **Adaptive break duration** from cluster signal quality.
8. ~~Sticky half-open with configurable probe parallelism~~ — **done**: single-flight probe per lease owner.

---

## File map

```
src/Polly.Core/CircuitBreaker/Distributed/
  IDistributedCircuitStateStore.cs
  InMemoryDistributedCircuitStateStore.cs
  DistributedCircuitSnapshot.cs
  DistributedHealthContribution.cs
  DistributedHealthAggregate.cs
  DistributedCircuitPartitionPolicy.cs
  DistributedCircuitBreakerStrategyOptions.TResult.cs
  DistributedCircuitBreakerResilienceStrategy.cs
  DistributedCircuitBreakerResiliencePipelineBuilderExtensions.cs
  DistributedCircuitBreakerConstants.cs

test/Polly.Core.Tests/CircuitBreaker/Distributed/
  InMemoryDistributedCircuitStateStoreTests.cs
  DistributedCircuitBreakerResilienceStrategyTests.cs

docs/strategies/distributed-circuit-breaker.md
```
