using Polly.Telemetry;

namespace Polly.CircuitBreaker.Distributed;

/// <summary>
/// Distributed circuit breaker that coordinates state through <see cref="IDistributedCircuitStateStore"/>.
/// </summary>
internal sealed class DistributedCircuitBreakerResilienceStrategy<T> : ResilienceStrategy<T>
{
    private readonly object _localSync = new();
    private readonly IDistributedCircuitStateStore _store;
    private readonly TimeProvider _timeProvider;
    private readonly ResilienceStrategyTelemetry _telemetry;
    private readonly Func<CircuitBreakerPredicateArguments<T>, ValueTask<bool>> _shouldHandle;
    private readonly string _circuitKey;
    private readonly string _instanceId;
    private readonly TimeSpan _breakDuration;
    private readonly TimeSpan _samplingDuration;
    private readonly double _failureRatio;
    private readonly int _minimumThroughput;
    private readonly int? _consecutiveFailureThreshold;
    private readonly TimeSpan _allowedClockSkew;
    private readonly TimeSpan _halfOpenLeaseDuration;
    private readonly TimeSpan _stateRefreshInterval;
    private readonly DistributedCircuitPartitionPolicy _partitionPolicy;
    private readonly int _maxCasAttempts;
    private readonly Func<OnCircuitOpenedArguments<T>, ValueTask>? _onOpened;
    private readonly Func<OnCircuitClosedArguments<T>, ValueTask>? _onClosed;
    private readonly Func<OnCircuitHalfOpenedArguments, ValueTask>? _onHalfOpened;

    // Local rolling counters (published for aggregation).
    private int _localSuccesses;
    private int _localFailures;
    private int _localConsecutiveFailures;
    private DateTimeOffset _localWindowStarted;

    private DistributedCircuitSnapshot _lastKnown = DistributedCircuitSnapshot.Closed;
    private DateTimeOffset _lastRefreshUtc = DateTimeOffset.MinValue;
    public DistributedCircuitBreakerResilienceStrategy(
        DistributedCircuitBreakerStrategyOptions<T> options,
        TimeProvider timeProvider,
        ResilienceStrategyTelemetry telemetry)
    {
        _store = options.StateStore ?? throw new ArgumentException("StateStore is required.", nameof(options));
        _timeProvider = timeProvider;
        _telemetry = telemetry;
        _shouldHandle = options.ShouldHandle;
        _circuitKey = options.CircuitKey;
        _instanceId = string.IsNullOrWhiteSpace(options.InstanceId) ? Guid.NewGuid().ToString("N") : options.InstanceId;
        _breakDuration = options.BreakDuration;
        _samplingDuration = options.SamplingDuration;
        _failureRatio = options.FailureRatio;
        _minimumThroughput = options.MinimumThroughput;
        _consecutiveFailureThreshold = options.ConsecutiveFailureThreshold;
        _allowedClockSkew = options.AllowedClockSkew;
        _halfOpenLeaseDuration = options.HalfOpenLeaseDuration;
        _stateRefreshInterval = options.StateRefreshInterval;
        _partitionPolicy = options.PartitionPolicy;
        _maxCasAttempts = options.MaxCasAttempts;
        _onOpened = options.OnOpened;
        _onClosed = options.OnClosed;
        _onHalfOpened = options.OnHalfOpened;
        _localWindowStarted = timeProvider.GetUtcNow();

        options.StateProvider?.Initialize(() =>
        {
            lock (_localSync)
            {
                return _lastKnown.State;
            }
        });

        InstanceId = _instanceId;
    }

    /// <summary>
    /// Gets the instance id used for leases and health publication.
    /// </summary>
    public string InstanceId { get; }

    /// <summary>
    /// Gets the last known snapshot (for tests/diagnostics).
    /// </summary>
    internal DistributedCircuitSnapshot LastKnownSnapshot
    {
        get
        {
            lock (_localSync)
            {
                return _lastKnown;
            }
        }
    }

    protected internal override async ValueTask<Outcome<T>> ExecuteCore<TState>(
        Func<ResilienceContext, TState, ValueTask<Outcome<T>>> callback,
        ResilienceContext context,
        TState state)
    {
        var pre = await OnPreExecuteAsync(context).ConfigureAwait(context.ContinueOnCapturedContext);
        if (pre is Outcome<T> blocked)
        {
            return blocked;
        }

        var started = _timeProvider.GetUtcNow();
        Outcome<T> outcome;
        try
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            outcome = await callback(context, state).ConfigureAwait(context.ContinueOnCapturedContext);
        }
#pragma warning disable CA1031
        catch (Exception ex)
        {
            outcome = new(ex);
        }
#pragma warning restore CA1031

        _ = _timeProvider.GetUtcNow() - started;

        var handled = await _shouldHandle(new CircuitBreakerPredicateArguments<T>(context, outcome))
            .ConfigureAwait(context.ContinueOnCapturedContext);

        if (handled)
        {
            await OnFailureAsync(outcome, context).ConfigureAwait(context.ContinueOnCapturedContext);
        }
        else
        {
            await OnSuccessAsync(outcome, context).ConfigureAwait(context.ContinueOnCapturedContext);
        }

        return outcome;
    }

    private async ValueTask<Outcome<T>?> OnPreExecuteAsync(ResilienceContext context)
    {
        var snapshot = await RefreshStateAsync(context.CancellationToken).ConfigureAwait(context.ContinueOnCapturedContext);
        var now = _timeProvider.GetUtcNow();

        switch (snapshot.State)
        {
            case CircuitState.Isolated:
                return Reject(new IsolatedCircuitException(), snapshot, now);

            case CircuitState.Open:
                if (IsStillOpen(snapshot, now))
                {
                    return Reject(CreateBroken(snapshot, now), snapshot, now);
                }

                // Break window elapsed (with skew): try to acquire half-open lease.
                if (await TryAcquireHalfOpenLeaseAsync(snapshot, context).ConfigureAwait(context.ContinueOnCapturedContext))
                {
                    return null; // probe allowed
                }

                // Another instance holds the lease or won the race — reject.
                snapshot = await RefreshStateAsync(context.CancellationToken, force: true)
                    .ConfigureAwait(context.ContinueOnCapturedContext);
                if (snapshot.State is CircuitState.Closed or CircuitState.HalfOpen
                    && IsHalfOpenOwner(snapshot))
                {
                    return null;
                }

                return Reject(CreateBroken(snapshot, now), snapshot, now);

            case CircuitState.HalfOpen:
                if (IsHalfOpenOwner(snapshot) && !IsHalfOpenLeaseExpired(snapshot, now))
                {
                    return null;
                }

                if (IsHalfOpenLeaseExpired(snapshot, now))
                {
                    // Lease abandoned — reopen so a new probe can be scheduled.
                    await TryReopenAfterAbandonedLeaseAsync(snapshot, context).ConfigureAwait(context.ContinueOnCapturedContext);
                }

                return Reject(CreateBroken(snapshot, now), snapshot, now);

            default:
                return null;
        }
    }

    private async ValueTask OnSuccessAsync(Outcome<T> outcome, ResilienceContext context)
    {
        RecordLocal(success: true);
        await PublishHealthAsync(context.CancellationToken).ConfigureAwait(context.ContinueOnCapturedContext);

        var snapshot = await RefreshStateAsync(context.CancellationToken).ConfigureAwait(context.ContinueOnCapturedContext);
        if (snapshot.State == CircuitState.HalfOpen && IsHalfOpenOwner(snapshot))
        {
            await TryTransitionAsync(
                snapshot,
                nextFactory: (current, utc) => current.WithNextVersion(
                    CircuitState.Closed,
                    DateTimeOffset.MinValue,
                    utc,
                    halfOpenLeaseOwner: null,
                    halfOpenLeaseExpiresUtc: null,
                    lastError: null),
                context,
                onApplied: async (ctx, applied) =>
                {
                    _ = applied;
                    ResetLocalMetrics();
                    try
                    {
                        await _store.ClearHealthAsync(_circuitKey, ctx.CancellationToken)
                            .ConfigureAwait(ctx.ContinueOnCapturedContext);
                    }
#pragma warning disable CA1031
                    catch (Exception)
#pragma warning restore CA1031
                    {
                    }

                    var args = new OnCircuitClosedArguments<T>(ctx, outcome, isManual: false);
                    _telemetry.Report<OnCircuitClosedArguments<T>, T>(
                        new(ResilienceEventSeverity.Information, DistributedCircuitBreakerConstants.OnClosedEvent),
                        args);
                    if (_onClosed is not null)
                    {
                        await _onClosed(args).ConfigureAwait(ctx.ContinueOnCapturedContext);
                    }
                }).ConfigureAwait(context.ContinueOnCapturedContext);
            // Do not re-evaluate aggregated health on the same success that closed the circuit.
            return;
        }

        // Closed path: evaluate cluster health (slow path already published).
        await MaybeOpenFromAggregatedHealthAsync(context, outcome).ConfigureAwait(context.ContinueOnCapturedContext);
    }

    private async ValueTask OnFailureAsync(Outcome<T> outcome, ResilienceContext context)
    {
        RecordLocal(success: false);
        await PublishHealthAsync(context.CancellationToken).ConfigureAwait(context.ContinueOnCapturedContext);

        var snapshot = await RefreshStateAsync(context.CancellationToken).ConfigureAwait(context.ContinueOnCapturedContext);

        if (snapshot.State == CircuitState.HalfOpen && IsHalfOpenOwner(snapshot))
        {
            var error = outcome.Exception?.GetType().Name ?? "handled-failure";
            await TryTransitionAsync(
                snapshot,
                nextFactory: (current, utc) => current.WithNextVersion(
                    CircuitState.Open,
                    utc + _breakDuration,
                    utc,
                    halfOpenLeaseOwner: null,
                    halfOpenLeaseExpiresUtc: null,
                    lastError: error),
                context,
                onApplied: async (ctx, applied) =>
                {
                    await RaiseOpenedAsync(ctx, outcome, applied).ConfigureAwait(ctx.ContinueOnCapturedContext);
                }).ConfigureAwait(context.ContinueOnCapturedContext);
            return;
        }

        await MaybeOpenFromAggregatedHealthAsync(context, outcome).ConfigureAwait(context.ContinueOnCapturedContext);
    }

    private async ValueTask MaybeOpenFromAggregatedHealthAsync(ResilienceContext context, Outcome<T> outcome)
    {
        var now = _timeProvider.GetUtcNow();
        DistributedHealthAggregate aggregate;
        try
        {
            aggregate = await _store.GetAggregatedHealthAsync(
                _circuitKey,
                now,
                _samplingDuration,
                context.CancellationToken).ConfigureAwait(context.ContinueOnCapturedContext);
        }
#pragma warning disable CA1031
        catch (Exception)
#pragma warning restore CA1031
        {
            return; // cannot evaluate cluster health — do not open from partial data
        }

        var shouldTrip =
            (aggregate.Throughput >= _minimumThroughput && aggregate.FailureRate >= _failureRatio)
            || (_consecutiveFailureThreshold is int threshold && aggregate.MaxConsecutiveFailures >= threshold);

        if (!shouldTrip)
        {
            return;
        }

        var snapshot = await RefreshStateAsync(context.CancellationToken, force: true)
            .ConfigureAwait(context.ContinueOnCapturedContext);

        if (snapshot.State is CircuitState.Open or CircuitState.Isolated or CircuitState.HalfOpen)
        {
            return;
        }

        var error = outcome.Exception?.GetType().Name ?? "aggregated-failure";
        await TryTransitionAsync(
            snapshot,
            nextFactory: (current, utc) => current.WithNextVersion(
                CircuitState.Open,
                utc + _breakDuration,
                utc,
                halfOpenLeaseOwner: null,
                halfOpenLeaseExpiresUtc: null,
                lastError: error),
            context,
            onApplied: async (ctx, applied) =>
                await RaiseOpenedAsync(ctx, outcome, applied).ConfigureAwait(ctx.ContinueOnCapturedContext))
            .ConfigureAwait(context.ContinueOnCapturedContext);
    }

    private async ValueTask<bool> TryAcquireHalfOpenLeaseAsync(DistributedCircuitSnapshot openSnapshot, ResilienceContext context)
    {
        var now = _timeProvider.GetUtcNow();
        var next = openSnapshot.WithNextVersion(
            CircuitState.HalfOpen,
            openSnapshot.OpenUntilUtc,
            now,
            _instanceId,
            now + _halfOpenLeaseDuration,
            openSnapshot.LastError);

        for (var attempt = 0; attempt < _maxCasAttempts; attempt++)
        {
            bool updated;
            try
            {
                updated = await _store.TryUpdateAsync(_circuitKey, openSnapshot, next, context.CancellationToken)
                    .ConfigureAwait(context.ContinueOnCapturedContext);
            }
#pragma warning disable CA1031
            catch (Exception)
#pragma warning restore CA1031
            {
                return false;
            }

            if (updated)
            {
                Remember(next);
                var args = new OnCircuitHalfOpenedArguments(context);
                _telemetry.Report(
                    new(ResilienceEventSeverity.Warning, DistributedCircuitBreakerConstants.OnHalfOpenEvent),
                    context,
                    args);
                if (_onHalfOpened is not null)
                {
                    await _onHalfOpened(args).ConfigureAwait(context.ContinueOnCapturedContext);
                }

                return true;
            }

            openSnapshot = await RefreshStateAsync(context.CancellationToken, force: true)
                .ConfigureAwait(context.ContinueOnCapturedContext);
            now = _timeProvider.GetUtcNow();

            if (openSnapshot.State != CircuitState.Open || IsStillOpen(openSnapshot, now))
            {
                return openSnapshot.State == CircuitState.HalfOpen && IsHalfOpenOwner(openSnapshot);
            }

            next = openSnapshot.WithNextVersion(
                CircuitState.HalfOpen,
                openSnapshot.OpenUntilUtc,
                now,
                _instanceId,
                now + _halfOpenLeaseDuration,
                openSnapshot.LastError);
        }

        return false;
    }

    private async ValueTask TryReopenAfterAbandonedLeaseAsync(DistributedCircuitSnapshot halfOpen, ResilienceContext context)
    {
        var now = _timeProvider.GetUtcNow();
        var next = halfOpen.WithNextVersion(
            CircuitState.Open,
            now + _breakDuration,
            now,
            halfOpenLeaseOwner: null,
            halfOpenLeaseExpiresUtc: null,
            lastError: halfOpen.LastError ?? "half-open-lease-expired");

        try
        {
            if (await _store.TryUpdateAsync(_circuitKey, halfOpen, next, context.CancellationToken)
                    .ConfigureAwait(context.ContinueOnCapturedContext))
            {
                Remember(next);
            }
        }
#pragma warning disable CA1031
        catch (Exception)
#pragma warning restore CA1031
        {
            // ignore — next refresh will reconcile
        }
    }

    private async ValueTask TryTransitionAsync(
        DistributedCircuitSnapshot expected,
        Func<DistributedCircuitSnapshot, DateTimeOffset, DistributedCircuitSnapshot> nextFactory,
        ResilienceContext context,
        Func<ResilienceContext, DistributedCircuitSnapshot, ValueTask> onApplied)
    {
        var current = expected;
        for (var attempt = 0; attempt < _maxCasAttempts; attempt++)
        {
            var now = _timeProvider.GetUtcNow();
            var next = nextFactory(current, now);

            bool updated;
            try
            {
                updated = await _store.TryUpdateAsync(_circuitKey, current, next, context.CancellationToken)
                    .ConfigureAwait(context.ContinueOnCapturedContext);
            }
#pragma warning disable CA1031
            catch (Exception)
#pragma warning restore CA1031
            {
                return;
            }

            if (updated)
            {
                Remember(next);
                await onApplied(context, next).ConfigureAwait(context.ContinueOnCapturedContext);
                return;
            }

            current = await RefreshStateAsync(context.CancellationToken, force: true)
                .ConfigureAwait(context.ContinueOnCapturedContext);

            // Someone else already moved to a non-closed state while we tried to open, or closed while we tried to close.
            if (next.State == CircuitState.Open && current.State is CircuitState.Open or CircuitState.HalfOpen or CircuitState.Isolated)
            {
                return;
            }

            if (next.State == CircuitState.Closed && current.State == CircuitState.Closed)
            {
                return;
            }
        }
    }

    private async ValueTask RaiseOpenedAsync(ResilienceContext context, Outcome<T> outcome, DistributedCircuitSnapshot applied)
    {
        var args = new OnCircuitOpenedArguments<T>(context, outcome, _breakDuration, isManual: false);
        _telemetry.Report<OnCircuitOpenedArguments<T>, T>(
            new(ResilienceEventSeverity.Error, DistributedCircuitBreakerConstants.OnOpenedEvent),
            args);
        if (_onOpened is not null)
        {
            await _onOpened(args).ConfigureAwait(context.ContinueOnCapturedContext);
        }
    }

    private async ValueTask<DistributedCircuitSnapshot> RefreshStateAsync(CancellationToken cancellationToken, bool force = false)
    {
        var now = _timeProvider.GetUtcNow();
        lock (_localSync)
        {
            if (!force && now - _lastRefreshUtc < _stateRefreshInterval)
            {
                return _lastKnown;
            }
        }

        try
        {
            var remote = await _store.GetAsync(_circuitKey, cancellationToken).ConfigureAwait(false);
            var snapshot = remote ?? DistributedCircuitSnapshot.Closed;
            Remember(snapshot);
            return snapshot;
        }
#pragma warning disable CA1031
        catch (Exception)
#pragma warning restore CA1031
        {
            return ApplyPartitionPolicy();
        }
    }

    private DistributedCircuitSnapshot ApplyPartitionPolicy()
    {
        lock (_localSync)
        {
            return _partitionPolicy switch
            {
                DistributedCircuitPartitionPolicy.FailClosed => new DistributedCircuitSnapshot(
                    CircuitState.Open,
                    _lastKnown.Version,
                    _timeProvider.GetUtcNow() + _breakDuration,
                    _lastKnown.UpdatedAtUtc,
                    null,
                    null,
                    "store-partition"),
                DistributedCircuitPartitionPolicy.FailOpen => DistributedCircuitSnapshot.Closed,
                _ => _lastKnown,
            };
        }
    }

    private void Remember(DistributedCircuitSnapshot snapshot)
    {
        lock (_localSync)
        {
            _lastKnown = snapshot;
            _lastRefreshUtc = _timeProvider.GetUtcNow();
        }
    }

    private void RecordLocal(bool success)
    {
        var now = _timeProvider.GetUtcNow();
        lock (_localSync)
        {
            if (now - _localWindowStarted > _samplingDuration)
            {
                _localSuccesses = 0;
                _localFailures = 0;
                _localWindowStarted = now;
                // keep consecutive counter across window reset (matches multi-dimension CB intent)
            }

            if (success)
            {
                _localSuccesses++;
                _localConsecutiveFailures = 0;
            }
            else
            {
                _localFailures++;
                _localConsecutiveFailures++;
            }
        }
    }

    private void ResetLocalMetrics()
    {
        lock (_localSync)
        {
            _localSuccesses = 0;
            _localFailures = 0;
            _localConsecutiveFailures = 0;
            _localWindowStarted = _timeProvider.GetUtcNow();
        }
    }

    private async ValueTask PublishHealthAsync(CancellationToken cancellationToken)
    {
        DistributedHealthContribution contribution;
        lock (_localSync)
        {
            contribution = new DistributedHealthContribution(
                _instanceId,
                _localSuccesses,
                _localFailures,
                _localConsecutiveFailures,
                _timeProvider.GetUtcNow());
        }

        try
        {
            await _store.PublishHealthAsync(_circuitKey, contribution, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031
        catch (Exception)
#pragma warning restore CA1031
        {
            // best-effort publication
        }
    }

    private bool IsStillOpen(DistributedCircuitSnapshot snapshot, DateTimeOffset now) =>
        // Extend open window by allowed skew so lagging clocks do not probe early.
        now < snapshot.OpenUntilUtc + _allowedClockSkew;

    private bool IsHalfOpenOwner(DistributedCircuitSnapshot snapshot) =>
        string.Equals(snapshot.HalfOpenLeaseOwner, _instanceId, StringComparison.Ordinal);

    private static bool IsHalfOpenLeaseExpired(DistributedCircuitSnapshot snapshot, DateTimeOffset now) =>
        snapshot.HalfOpenLeaseExpiresUtc is { } expires && now >= expires;

    private Outcome<T> Reject(ExecutionRejectedException exception, DistributedCircuitSnapshot snapshot, DateTimeOffset now)
    {
        _ = snapshot;
        _ = now;
        _telemetry.SetTelemetrySource(exception);
        return Outcome.FromException<T>(exception.TrySetStackTrace());
    }

    private BrokenCircuitException CreateBroken(DistributedCircuitSnapshot snapshot, DateTimeOffset now)
    {
        var retryAfter = snapshot.OpenUntilUtc + _allowedClockSkew - now;
        if (retryAfter < TimeSpan.Zero)
        {
            retryAfter = TimeSpan.Zero;
        }

        return new BrokenCircuitException(BrokenCircuitException.DefaultMessage, retryAfter);
    }
}
