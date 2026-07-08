namespace Polly.CircuitBreaker.Distributed;

/// <summary>
/// Process-local implementation of <see cref="IDistributedCircuitStateStore"/> for tests and single-process multi-pipeline demos.
/// </summary>
/// <remarks>
/// Multiple strategy instances that share the same store instance behave like co-located nodes
/// coordinating through a shared backend. Not durable across process restarts.
/// <para>
/// Not suitable as a multi-tenant or cross-trust boundary store. Bound by
/// <see cref="MaxTrackedCircuitKeys"/> to limit unbounded key growth.
/// </para>
/// </remarks>
public sealed class InMemoryDistributedCircuitStateStore : IDistributedCircuitStateStore
{
    /// <summary>
    /// Default maximum number of distinct circuit keys retained by the store.
    /// </summary>
    public const int DefaultMaxTrackedCircuitKeys = 10_000;

    private readonly object _sync = new();
    private readonly Dictionary<string, DistributedCircuitSnapshot> _states = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Dictionary<string, DistributedHealthContribution>> _health =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryDistributedCircuitStateStore"/> class
    /// with <see cref="DefaultMaxTrackedCircuitKeys"/>.
    /// </summary>
    public InMemoryDistributedCircuitStateStore()
        : this(DefaultMaxTrackedCircuitKeys)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryDistributedCircuitStateStore"/> class.
    /// </summary>
    /// <param name="maxTrackedCircuitKeys">
    /// Maximum number of distinct circuit keys allowed (states + health). Prevents unbounded memory growth
    /// when keys are dynamic or untrusted.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="maxTrackedCircuitKeys"/> is less than 1.</exception>
    public InMemoryDistributedCircuitStateStore(int maxTrackedCircuitKeys)
    {
        if (maxTrackedCircuitKeys < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxTrackedCircuitKeys), "Must be at least 1.");
        }

        MaxTrackedCircuitKeys = maxTrackedCircuitKeys;
    }

    /// <summary>
    /// Gets the maximum number of distinct circuit keys this store will track.
    /// </summary>
    public int MaxTrackedCircuitKeys { get; }

    /// <inheritdoc />
    public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken)
    {
        Guard.NotNull(circuitKey);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            return _states.TryGetValue(circuitKey, out var snapshot)
                ? new ValueTask<DistributedCircuitSnapshot?>(snapshot)
                : new ValueTask<DistributedCircuitSnapshot?>((DistributedCircuitSnapshot?)null);
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> TryUpdateAsync(
        string circuitKey,
        DistributedCircuitSnapshot expected,
        DistributedCircuitSnapshot updated,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(circuitKey);
        cancellationToken.ThrowIfCancellationRequested();

        if (updated.Version != expected.Version + 1)
        {
            return new ValueTask<bool>(false);
        }

        lock (_sync)
        {
            if (_states.TryGetValue(circuitKey, out var current))
            {
                if (current.Version != expected.Version)
                {
                    return new ValueTask<bool>(false);
                }
            }
            else if (expected.Version != 0)
            {
                return new ValueTask<bool>(false);
            }
            else
            {
                EnsureCapacityForNewKey_NeedsLock(circuitKey);
            }

            _states[circuitKey] = updated;
            return new ValueTask<bool>(true);
        }
    }

    /// <inheritdoc />
    public ValueTask PublishHealthAsync(
        string circuitKey,
        DistributedHealthContribution contribution,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(circuitKey);
        Guard.NotNull(contribution.InstanceId);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            if (!_health.TryGetValue(circuitKey, out var byInstance))
            {
                EnsureCapacityForNewKey_NeedsLock(circuitKey);
                byInstance = new Dictionary<string, DistributedHealthContribution>(StringComparer.Ordinal);
                _health[circuitKey] = byInstance;
            }

            byInstance[contribution.InstanceId] = contribution;
        }

        return default;
    }

    /// <inheritdoc />
    public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(
        string circuitKey,
        DateTimeOffset utcNow,
        TimeSpan maxAge,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(circuitKey);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            if (!_health.TryGetValue(circuitKey, out var byInstance) || byInstance.Count == 0)
            {
                return new ValueTask<DistributedHealthAggregate>(new DistributedHealthAggregate(0, 0, 0, 0));
            }

            long success = 0;
            long failure = 0;
            var instances = 0;
            var maxConsecutive = 0;
            var stale = new List<string>();

            foreach (var pair in byInstance)
            {
                var c = pair.Value;
                if (utcNow - c.ReportedAtUtc > maxAge)
                {
                    stale.Add(pair.Key);
                    continue;
                }

                // long accumulator cannot overflow from int contributions in practical cluster sizes;
                // clamp when projecting back to the public int aggregate fields.
                success += c.SuccessCount;
                failure += c.FailureCount;
                instances++;
                if (c.ConsecutiveFailureCount > maxConsecutive)
                {
                    maxConsecutive = c.ConsecutiveFailureCount;
                }
            }

            foreach (var key in stale)
            {
                byInstance.Remove(key);
            }

            return new ValueTask<DistributedHealthAggregate>(
                new DistributedHealthAggregate(
                    ToSaturatedInt(success),
                    ToSaturatedInt(failure),
                    instances,
                    maxConsecutive));
        }
    }

    /// <inheritdoc />
    public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken)
    {
        Guard.NotNull(circuitKey);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            _health.Remove(circuitKey);
        }

        return default;
    }

    /// <summary>
    /// Removes all state (tests).
    /// </summary>
    public void Clear()
    {
        lock (_sync)
        {
            _states.Clear();
            _health.Clear();
        }
    }

    private static int ToSaturatedInt(long value) =>
        value > int.MaxValue ? int.MaxValue : (int)value;

    private void EnsureCapacityForNewKey_NeedsLock(string circuitKey)
    {
        if (_states.ContainsKey(circuitKey) || _health.ContainsKey(circuitKey))
        {
            return;
        }

        // Count union of keys roughly via max of the two maps (keys usually appear in both).
        var tracked = _states.Count;
        foreach (var key in _health.Keys)
        {
            if (!_states.ContainsKey(key))
            {
                tracked++;
            }
        }

        if (tracked >= MaxTrackedCircuitKeys)
        {
            throw new InvalidOperationException(
                $"InMemoryDistributedCircuitStateStore reached MaxTrackedCircuitKeys ({MaxTrackedCircuitKeys}).");
        }
    }
}
