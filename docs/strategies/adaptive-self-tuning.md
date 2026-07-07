# Adaptive self-tuning strategies

This document describes the **adaptive / self-tuning** resilience strategies added to `Polly.Core`:

- **Thread-safe sliding window metrics** (`SlidingWindowMetrics`)
- **Self-tuning timeout** (`AddSelfTuningTimeout`)
- **Self-tuning retry** (`AddSelfTuningRetry`)

These strategies follow the **Polly v8+** model (pipeline builders, `ResilienceStrategy` / `ResilienceStrategy<T>`, `Outcome<T>`, telemetry, options validation). They are **additive**: existing public APIs and built-in strategies are unchanged.

---

## Motivation

Fixed timeouts and fixed retry budgets are hard to size for changing workloads:

| Problem | Fixed strategy pain | Adaptive approach |
|--------|---------------------|-------------------|
| Latency drifts up | Timeouts fire too often | Timeout tracks recent latency percentile |
| Latency improves | Timeouts are overly long | Timeout shrinks with observed latency |
| Downstream is mostly healthy | Retries are fine | Use full retry budget, shorter delay |
| Downstream is failing hard | Retries amplify load | Reduce attempts, increase delay |

---

## Architecture

```
                    ┌─────────────────────────────────────┐
                    │     ResiliencePipelineBuilder       │
                    │  .AddSelfTuningRetry(...)           │
                    │  .AddSelfTuningTimeout(...)         │
                    └───────────────┬─────────────────────┘
                                    │ Build()
                                    ▼
                    ┌─────────────────────────────────────┐
                    │         ResiliencePipeline          │
                    │  CompositeComponent / Delegating    │
                    └───────────────┬─────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          ▼                         ▼                         ▼
┌──────────────────┐    ┌──────────────────────┐   ┌──────────────────┐
│ SelfTuningRetry  │    │ SelfTuningTimeout    │   │  User callback   │
│ ResilienceStrategy│───▶│ ResilienceStrategy   │──▶│                  │
└────────┬─────────┘    └──────────┬───────────┘   └──────────────────┘
         │                         │
         │      optional share     │
         └───────────┬─────────────┘
                     ▼
         ┌───────────────────────┐
         │  SlidingWindowMetrics │
         │  (thread-safe ring)   │
         └───────────────────────┘
```

### Components

| Type | Visibility | Responsibility |
|------|------------|----------------|
| `SlidingWindowMetrics` | **public** | Thread-safe ring buffer of `(timestamp, duration, success)`. Evicts by time window and capacity. Exposes snapshot + latency percentiles. |
| `SlidingWindowSnapshot` | **public** | Immutable stats: counts, rates, min/avg/max duration. |
| `SelfTuningTimeoutStrategyOptions` | **public** | Configuration for adaptive timeout. |
| `SelfTuningTimeoutResilienceStrategy` | **internal** | Proactive strategy: applies adaptive CTS timeout; records latency; maps cancel → `TimeoutRejectedException`. |
| `SelfTuningRetryStrategyOptions` / `<TResult>` | **public** | Configuration for adaptive retry. |
| `SelfTuningRetryResilienceStrategy<T>` | **internal** | Reactive strategy: adapts max attempts + base delay from failure rate; reuses Polly delay/jitter helpers. |
| `AddSelfTuningTimeout` / `AddSelfTuningRetry` | **public** extensions on builders | v8 registration entry points (same pattern as `AddTimeout` / `AddRetry`). |

### Execution contract (v8 compliance)

- Strategies implement `ResilienceStrategy` (timeout) or `ResilienceStrategy<T>` (retry).
- Callbacks surface as `Outcome<T>` (exceptions captured, not used for control-flow throws inside the chain).
- Builder methods call `AddStrategy(...)` with validated `ResilienceStrategyOptions`.
- Telemetry uses `ResilienceStrategyTelemetry` (`OnTimeout`, `OnRetry`, execution attempt events).
- Time is abstracted via `TimeProvider` (testable with `FakeTimeProvider`).

### Sliding window metrics (thread-safety)

`SlidingWindowMetrics` is the shared observation layer:

1. **Lock** protects the ring buffer (simple, correct under high concurrency).
2. Each `Record(duration, success)` stores UTC timestamp, duration ticks, and success flag.
3. **Time eviction**: samples older than `SamplingWindow` are dropped on read/write.
4. **Capacity eviction**: when full, the oldest slot is overwritten.
5. **Percentiles**: copy live samples under the lock, sort the prefix, nearest-rank selection.

Strategies may:

- create a **private** metrics instance from options (`SamplingWindow`, `Capacity`), or
- inject a **shared** `SlidingWindowMetrics` via `options.Metrics` so timeout and retry co-tune from the same traffic.

---

## Self-tuning timeout

### Algorithm

```
if sampleCount < MinimumSamples:
    timeout = InitialTimeout
else:
    latency = GetLatencyPercentile(LatencyPercentile)   # e.g. P99
    timeout = latency * TimeoutMultiplier
timeout = clamp(timeout, MinTimeout, MaxTimeout)
```

On each execution:

1. Compute adaptive timeout.
2. Link a timeout CTS (same approach as built-in timeout strategy).
3. On **timeout**: record failure, raise `OnTimeout`, return `TimeoutRejectedException`.
4. On **completion**: record duration and success/failure.

### Options (`SelfTuningTimeoutStrategyOptions`)

| Property | Default | Meaning |
|----------|---------|---------|
| `InitialTimeout` | 30s | Used until `MinimumSamples` observed |
| `MinTimeout` / `MaxTimeout` | 50ms / 60s | Hard clamps |
| `LatencyPercentile` | 0.99 | Percentile of recent durations |
| `TimeoutMultiplier` | 2.0 | Headroom over observed latency |
| `SamplingWindow` | 30s | How long samples remain relevant |
| `Capacity` | 256 | Max samples retained |
| `MinimumSamples` | 20 | Warm-up before adapting |
| `Metrics` | `null` | Optional shared metrics |
| `OnTimeout` | `null` | Callback when timeout fires |

### Usage

```csharp
using Polly;
using Polly.Adaptive;

var pipeline = new ResiliencePipelineBuilder()
    .AddSelfTuningTimeout(new SelfTuningTimeoutStrategyOptions
    {
        InitialTimeout = TimeSpan.FromSeconds(3),
        MinTimeout = TimeSpan.FromMilliseconds(100),
        MaxTimeout = TimeSpan.FromSeconds(10),
        LatencyPercentile = 0.99,
        TimeoutMultiplier = 2.0,
        MinimumSamples = 20,
        OnTimeout = args =>
        {
            Console.WriteLine($"Adaptive timeout {args.Timeout}");
            return default;
        }
    })
    .Build();

await pipeline.ExecuteAsync(async token =>
{
    // work that should not run forever
    await DoWorkAsync(token);
}, cancellationToken);
```

---

## Self-tuning retry

### Algorithm

After warm-up (`MinimumSamples`):

- Let `f` = recent **failure rate** (share of handled/failed attempts in the window).
- Let `T` = `HighFailureRateThreshold` (default `0.5`).

**When `f < T` (system relatively healthy):**

- `MaxRetryAttempts` = configured maximum (favor recovering rare transients).
- Base delay interpolates from `MinDelay` toward the midpoint of `[MinDelay, MaxDelay]` as `f` approaches `T`.

**When `f ≥ T` (system unhealthy):**

- Reduce attempts from `MaxRetryAttempts` toward `MinRetryAttempts` as `f` approaches `1`.
- Increase base delay from the midpoint toward `MaxDelay` (back off harder, protect the dependency).

Per attempt, delays use existing `RetryHelper` with `BackoffType` and optional jitter (same formulas as classic retry).

Each attempt records `(duration, success: !shouldHandle)` so the window reflects real pressure under retries.

### Options (`SelfTuningRetryStrategyOptions` / `<TResult>`)

| Property | Default | Meaning |
|----------|---------|---------|
| `InitialRetryAttempts` | 3 | Warm-up attempts |
| `MinRetryAttempts` / `MaxRetryAttempts` | 1 / 5 | Adaptive range |
| `InitialDelay` | 1s | Warm-up base delay |
| `MinDelay` / `MaxDelay` | 100ms / 30s | Adaptive delay bounds |
| `BackoffType` | Exponential | Per-attempt schedule |
| `UseJitter` | `true` | Decorrelated jitter |
| `HighFailureRateThreshold` | 0.5 | Load-shedding pivot |
| `SamplingWindow` / `Capacity` / `MinimumSamples` | 30s / 256 / 20 | Metrics window |
| `ShouldHandle` | any exception except cancel | What counts as retryable |
| `Metrics` | `null` | Optional shared metrics |
| `OnRetry` | `null` | Callback before delay |

### Usage

```csharp
using Polly;
using Polly.Adaptive;

var pipeline = new ResiliencePipelineBuilder()
    .AddSelfTuningRetry(new SelfTuningRetryStrategyOptions
    {
        InitialRetryAttempts = 3,
        MinRetryAttempts = 1,
        MaxRetryAttempts = 5,
        InitialDelay = TimeSpan.FromMilliseconds(200),
        MinDelay = TimeSpan.FromMilliseconds(100),
        MaxDelay = TimeSpan.FromSeconds(5),
        HighFailureRateThreshold = 0.5,
        ShouldHandle = new PredicateBuilder().Handle<HttpRequestException>(),
        OnRetry = args =>
        {
            Console.WriteLine(
                $"Retry #{args.AttemptNumber}, delay={args.RetryDelay}, budget={args.MaxRetryAttempts}");
            return default;
        }
    })
    .Build();
```

Generic result pipelines:

```csharp
var pipeline = new ResiliencePipelineBuilder<HttpResponseMessage>()
    .AddSelfTuningRetry(new SelfTuningRetryStrategyOptions<HttpResponseMessage>
    {
        ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
            .Handle<HttpRequestException>()
            .HandleResult(r => (int)r.StatusCode >= 500)
    })
    .Build();
```

---

## Sharing metrics (recommended composition)

```csharp
var metrics = new SlidingWindowMetrics(
    samplingWindow: TimeSpan.FromSeconds(30),
    capacity: 512);

var pipeline = new ResiliencePipelineBuilder()
    .AddSelfTuningRetry(new SelfTuningRetryStrategyOptions
    {
        Metrics = metrics,
        // ...
    })
    .AddSelfTuningTimeout(new SelfTuningTimeoutStrategyOptions
    {
        Metrics = metrics,
        // ...
    })
    .Build();

// Optional: observe tuning state for diagnostics
var snapshot = metrics.GetSnapshot();
Console.WriteLine($"n={snapshot.SampleCount} failRate={snapshot.FailureRate:P0} avg={snapshot.AverageDuration}");
```

**Order note:** Place **retry outside timeout** (retry first in the builder = outer) if you want each attempt to have its own adaptive timeout. Place **timeout outside retry** to bound the entire retry budget.

```csharp
// Outer retry, inner per-attempt timeout (common)
new ResiliencePipelineBuilder()
    .AddSelfTuningRetry(...)
    .AddSelfTuningTimeout(...)
    .Build();
```

---

## Backward compatibility

| Guarantee | How |
|-----------|-----|
| Existing public API unchanged | Only **new** types and extension methods added |
| No change to built-in timeout/retry | Separate classes under `Polly.Adaptive` |
| Package consumers opt-in | Must call `AddSelfTuning*` explicitly |
| Public API analyzers | New surface listed in `PublicAPI.Unshipped.txt` |

---

## Testing

Unit tests live under `test/Polly.Core.Tests/Adaptive/`:

- `SlidingWindowMetricsTests` — concurrency, expiry, capacity, percentiles
- `SelfTuningTimeoutResilienceStrategyTests` — warm-up, clamp, timeout path, tuning
- `SelfTuningRetryResilienceStrategyTests` — adaptive parameters, retry loops, shared metrics

```bash
cd Polly
export PATH="$HOME/.dotnet:$PATH"
export DOTNET_ROOT="$HOME/.dotnet"

dotnet test test/Polly.Core.Tests/Polly.Core.Tests.csproj \
  -f net10.0 \
  -p:CollectCoverage=false \
  --filter "FullyQualifiedName~Adaptive"
```

---

## Design choices and limits

1. **Ring buffer + lock** — predictable and correct; capacity keeps CPU for percentile sorts bounded.
2. **Warm-up samples** — avoid adapting on noise at process start.
3. **High failure rate reduces retries** — prefers stability over hammering a broken dependency (opposite of “always retry more”).
4. **Not a replacement for circuit breaker** — pair with `AddCircuitBreaker` when you need hard open/half-open isolation.
5. **Metrics are attempt-scoped for retry** — every attempt updates the window so sustained failure is visible quickly.

---

## File map

```
src/Polly.Core/Adaptive/
  AdaptiveConstants.cs
  SlidingWindowMetrics.cs
  SlidingWindowSnapshot.cs
  SelfTuningTimeoutStrategyOptions.cs
  SelfTuningTimeoutResilienceStrategy.cs
  SelfTuningTimeoutResiliencePipelineBuilderExtensions.cs
  OnSelfTuningTimeoutArguments.cs
  SelfTuningRetryStrategyOptions.TResult.cs
  SelfTuningRetryResilienceStrategy.cs
  SelfTuningRetryResiliencePipelineBuilderExtensions.cs
  OnSelfTuningRetryArguments.cs

test/Polly.Core.Tests/Adaptive/
  SlidingWindowMetricsTests.cs
  SelfTuningTimeoutResilienceStrategyTests.cs
  SelfTuningRetryResilienceStrategyTests.cs

docs/strategies/adaptive-self-tuning.md   ← this file
```
