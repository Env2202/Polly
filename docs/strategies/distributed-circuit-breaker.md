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
- **Half-open race**: exactly one instance may CAS from `Open` → `HalfOpen` with `HalfOpenLeaseOwner = instanceId` and a lease expiry. Others keep rejecting until the owner closes or the lease is abandoned (re-open).
- **Close**: only the lease owner closes on a successful probe (`HalfOpen` → `Closed`).

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
- Readers apply `AllowedClockSkew`:
  - Still open while `now < OpenUntilUtc + AllowedClockSkew` (prevents early probes on lagging clocks).
- Prefer NTP / cloud time sync in production; skew is a safety margin, not a full time-sync protocol.

### 3. Half-open races

- Optimistic concurrency via **monotonic `Version`** on every write (`next.Version == expected.Version + 1`).
- Half-open **lease** binds the probe to one `InstanceId` with `HalfOpenLeaseExpiresUtc`.
- Expired lease → CAS back to `Open` so another instance can probe (avoids permanent half-open deadlock if the owner dies).

### 4. Health metrics aggregation

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
  || (ConsecutiveFailureThreshold is set && MaxConsecutiveFailures >= threshold)
```

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
| `ConsecutiveFailureThreshold` | null | Optional max consecutive across instances |
| `AllowedClockSkew` | 2s | Extends open window interpretation |
| `HalfOpenLeaseDuration` | 5s | Probe ownership TTL |
| `StateRefreshInterval` | 100ms | Hot-path refresh throttle |
| `PartitionPolicy` | PreferLastKnownState | Store outage behavior |
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
8. **Sticky half-open** with configurable probe parallelism (today: exactly one).

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
