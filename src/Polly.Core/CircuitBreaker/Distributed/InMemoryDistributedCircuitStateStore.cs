namespace Polly.CircuitBreaker.Distributed;

/// <summary>
/// Process-local implementation of <see cref="IDistributedCircuitStateStore"/> for tests and single-process multi-pipeline demos.
/// </summary>
/// <remarks>
/// Multiple strategy instances that share the same store instance behave like co-located nodes
/// coordinating through a shared backend. Not durable across process restarts.
/// </remarks>
public sealed class InMemoryDistributedCircuitStateStore : IDistributedCircuitStateStore
{
    private readonly object _sync = new();
    private readonly Dictionary<string, DistributedCircuitSnapshot> _states = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Dictionary<string, DistributedHealthContribution>> _health =
        new(StringComparer.Ordinal);

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
        DistributedCircuitSnapshot next,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(circuitKey);
        cancellationToken.ThrowIfCancellationRequested();

        if (next.Version != expected.Version + 1)
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

            _states[circuitKey] = next;
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

            var success = 0;
            var failure = 0;
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
                new DistributedHealthAggregate(success, failure, instances, maxConsecutive));
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
}
