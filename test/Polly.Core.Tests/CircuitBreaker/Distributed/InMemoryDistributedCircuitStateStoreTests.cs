using Microsoft.Extensions.Time.Testing;
using Polly.CircuitBreaker;
using Polly.CircuitBreaker.Distributed;

namespace Polly.Core.Tests.CircuitBreaker.Distributed;

public class InMemoryDistributedCircuitStateStoreTests
{
    [Fact]
    public async Task GetAsync_Missing_ReturnsNull()
    {
        var store = new InMemoryDistributedCircuitStateStore();
        (await store.GetAsync("k", CancellationToken.None)).ShouldBeNull();
    }

    [Fact]
    public async Task TryUpdate_Cas_SucceedsAndRejectsStale()
    {
        var store = new InMemoryDistributedCircuitStateStore();
        var closed = DistributedCircuitSnapshot.Closed;
        var open = closed.WithNextVersion(
            CircuitState.Open,
            DateTimeOffset.UtcNow.AddSeconds(5),
            DateTimeOffset.UtcNow,
            null,
            null,
            "err");

        (await store.TryUpdateAsync("k", closed, open, CancellationToken.None)).ShouldBeTrue();
        (await store.GetAsync("k", CancellationToken.None))!.Value.State.ShouldBe(CircuitState.Open);

        var stale = closed.WithNextVersion(
            CircuitState.Closed,
            DateTimeOffset.MinValue,
            DateTimeOffset.UtcNow,
            null,
            null,
            null);
        (await store.TryUpdateAsync("k", closed, stale, CancellationToken.None)).ShouldBeFalse();
    }

    [Fact]
    public async Task Health_AggregatesAndExpires()
    {
        var time = new FakeTimeProvider();
        var store = new InMemoryDistributedCircuitStateStore();

        await store.PublishHealthAsync("k", new DistributedHealthContribution("a", 8, 2, 0, time.GetUtcNow()), CancellationToken.None);
        await store.PublishHealthAsync("k", new DistributedHealthContribution("b", 1, 9, 5, time.GetUtcNow()), CancellationToken.None);

        var aggregate = await store.GetAggregatedHealthAsync("k", time.GetUtcNow(), TimeSpan.FromSeconds(30), CancellationToken.None);
        aggregate.SuccessCount.ShouldBe(9);
        aggregate.FailureCount.ShouldBe(11);
        aggregate.ContributingInstances.ShouldBe(2);
        aggregate.MaxConsecutiveFailures.ShouldBe(5);
        aggregate.Throughput.ShouldBe(20);
        aggregate.FailureRate.ShouldBe(0.55, 0.0001);

        time.Advance(TimeSpan.FromSeconds(40));
        var expired = await store.GetAggregatedHealthAsync("k", time.GetUtcNow(), TimeSpan.FromSeconds(30), CancellationToken.None);
        expired.ContributingInstances.ShouldBe(0);
    }
}
