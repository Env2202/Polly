# Polly Multi-Dimension Circuit Breaker — Working Notes

Source: Notion [Project Proposals - HARD](https://app.notion.com/p/372dc69ccaf48163aec1d92942ac7ed2) + Work Log task [Polly multi-dimension circuit breaker](https://app.notion.com/p/37adc69ccaf4817d94acf64e92ebae57)

Repo: https://github.com/App-vNext/Polly (local: `Polly/`)

## Goal

Extend advanced circuit breaker beyond **rolling failure rate** so any of three independent dimensions can trip the circuit:

| Dimension | Trip condition (when enabled) |
|-----------|-------------------------------|
| **Failure rate** (existing) | `Throughput >= MinimumThroughput` AND `FailureRate >= FailureRatio` |
| **Slow call rate** (new) | `Throughput >= MinimumThroughput` AND `SlowCallRate >= SlowCallRateThreshold` |
| **Consecutive failures** (new) | `ConsecutiveFailureCount >= ConsecutiveFailureThreshold` |

Unify in existing `AdvancedCircuitBehavior` + `HealthMetrics` / `HealthInfo` — **no** separate `CircuitBehavior` subclass for consecutive.

## Constraints from task spec

1. All three dimensions in `RollingHealthMetrics` and `SingleHealthMetrics` (in sync).
2. Slow-call duration threaded through the **success** path (call completed successfully but was slow).
3. Flow metrics through `HealthInfo` (extend existing shape).
4. Any single dimension can open the circuit independently (OR trip logic).
5. Exact timeline parity tests with `FakeTimeProvider` — per dimension + mixed (only consecutive trips).
6. Existing tests pass **without modification**.
7. Write-up: architecture, commands to run tests.

## Proposed options API (`CircuitBreakerStrategyOptions<TResult>`)

| Property | Default | Meaning |
|----------|---------|---------|
| `FailureRatio` | `0.1` | Existing |
| `MinimumThroughput` | `100` | Existing |
| `SamplingDuration` | `30s` | Existing |
| `SlowCallDurationThreshold` | `null` (disabled) | Successes slower than this count as slow |
| `SlowCallRateThreshold` | `null` (disabled) | Trip when slow-call rate >= this (0–1) |
| `ConsecutiveFailureThreshold` | `null` (disabled) | Trip when consecutive handled failures >= this |

Disabled = `null` so existing behavior unchanged.

## Implementation plan

1. Extend `HealthInfo` with slow-call + consecutive fields.
2. Extend `HealthMetrics` / implementations: track slow successes, consecutive counter reset on success.
3. Thread `TimeSpan duration` through success path: strategy → controller → behavior → metrics.
4. Options + `AdvancedCircuitBehavior` OR trip logic.
5. Unit tests (new only).
6. Implementation write-up section below (filled after code).

## Build / test commands (local)

```bash
cd Polly
dotnet build src/Polly.Core/Polly.Core.csproj
dotnet test test/Polly.Core.Tests/Polly.Core.Tests.csproj --filter "FullyQualifiedName~CircuitBreaker" -p:CollectCoverage=false
```

## Architecture (implemented)

```
CircuitBreakerResilienceStrategy
  ├─ measures call duration via TimeProvider (success path only)
  └─ OnUnhandledOutcomeAsync(outcome, context, duration)
         └─ CircuitStateController
                ├─ OnActionSuccess(state, duration)
                │     └─ AdvancedCircuitBehavior
                │            └─ HealthMetrics.IncrementSuccess(duration)
                │                   └─ resets consecutive; may count slow call
                └─ if Closed && AdvancedCircuitBehavior.ShouldBreakOnSuccess()
                       └─ open circuit (slow-call dimension)

  OnHandledOutcomeAsync
         └─ AdvancedCircuitBehavior.OnActionFailure
                └─ IncrementFailure + ShouldTrip(info)
                       OR: failure-rate | slow-call-rate | consecutive
```

### Key files changed

| File | Change |
|------|--------|
| `Health/HealthInfo.cs` | Added `SlowCallCount`, `SlowCallRate`, `ConsecutiveFailureCount` (defaults keep existing tests compiling) |
| `Health/HealthMetrics.cs` | `IncrementSuccess(TimeSpan? duration)`, `ConfigureSlowCall` |
| `Health/SingleHealthMetrics.cs` | 3 dimensions; consecutive survives window reset |
| `Health/RollingHealthMetrics.cs` | Per-window slow calls; consecutive is non-windowed |
| `Controller/CircuitBehavior.cs` | Optional duration on success |
| `Controller/AdvancedCircuitBehavior.cs` | OR trip logic; `ShouldBreakOnSuccess` for slow-call |
| `Controller/CircuitStateController.cs` | Passes duration; opens on slow-call success while closed |
| `CircuitBreakerResilienceStrategy.cs` | Times execution; passes duration on success |
| `CircuitBreakerStrategyOptions.TResult.cs` | `SlowCallDurationThreshold`, `SlowCallRateThreshold`, `ConsecutiveFailureThreshold` |
| `CircuitBreakerResiliencePipelineBuilderExtensions.cs` | Wires new options + time provider |

### Options (all opt-in; defaults preserve existing behavior)

```csharp
new CircuitBreakerStrategyOptions
{
    // existing
    FailureRatio = 0.5,
    MinimumThroughput = 100,
    SamplingDuration = TimeSpan.FromSeconds(30),

    // new — null = disabled
    SlowCallDurationThreshold = TimeSpan.FromMilliseconds(500),
    SlowCallRateThreshold = 0.5,          // trip if ≥50% of calls are slow
    ConsecutiveFailureThreshold = 5,      // trip on 5th consecutive handled failure
}
```

### New tests

`test/Polly.Core.Tests/CircuitBreaker/MultiDimension/MultiDimensionCircuitBreakerTests.cs`

- Metrics: slow-call + consecutive tracking (Single + Rolling)
- Behavior: each dimension trips independently via stubs
- Timelines (`FakeTimeProvider`): consecutive-only, slow-call-only, failure-rate-only, mixed (only consecutive)

### Verify locally

```bash
cd Polly
dotnet build src/Polly.Core/Polly.Core.csproj
dotnet test test/Polly.Core.Tests/Polly.Core.Tests.csproj \
  --filter "FullyQualifiedName~CircuitBreaker" \
  -p:CollectCoverage=false
```

`dotnet` was not available in this environment; run the above on your machine to confirm.
