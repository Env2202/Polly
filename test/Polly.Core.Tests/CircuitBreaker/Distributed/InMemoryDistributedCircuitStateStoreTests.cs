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

    [Fact]
    public async Task TryUpdate_RejectsNonMonotonicVersion()
    {
        var store = new InMemoryDistributedCircuitStateStore();
        var closed = DistributedCircuitSnapshot.Closed;
        var bad = new DistributedCircuitSnapshot(
            CircuitState.Open,
            version: 5,
            openUntilUtc: DateTimeOffset.UtcNow.AddSeconds(1),
            updatedAtUtc: DateTimeOffset.UtcNow,
            halfOpenLeaseOwner: null,
            halfOpenLeaseExpiresUtc: null,
            lastError: "x");

        (await store.TryUpdateAsync("k", closed, bad, CancellationToken.None)).ShouldBeFalse();
    }

    [Fact]
    public async Task TryUpdate_MissingKeyWithNonZeroExpected_Fails()
    {
        var store = new InMemoryDistributedCircuitStateStore();
        var expected = new DistributedCircuitSnapshot(
            CircuitState.Closed,
            version: 3,
            openUntilUtc: DateTimeOffset.MinValue,
            updatedAtUtc: DateTimeOffset.MinValue,
            halfOpenLeaseOwner: null,
            halfOpenLeaseExpiresUtc: null,
            lastError: null);
        var next = expected.WithNextVersion(
            CircuitState.Open,
            DateTimeOffset.UtcNow.AddSeconds(1),
            DateTimeOffset.UtcNow,
            null,
            null,
            "e");

        (await store.TryUpdateAsync("missing", expected, next, CancellationToken.None)).ShouldBeFalse();
    }

    [Fact]
    public async Task GetAggregatedHealth_Empty_ReturnsZeros()
    {
        var store = new InMemoryDistributedCircuitStateStore();
        var aggregate = await store.GetAggregatedHealthAsync("none", DateTimeOffset.UtcNow, TimeSpan.FromSeconds(1), CancellationToken.None);
        aggregate.Throughput.ShouldBe(0);
        aggregate.FailureRate.ShouldBe(0);
        aggregate.ContributingInstances.ShouldBe(0);
    }

    [Fact]
    public async Task Clear_AndClearHealth_RemoveState()
    {
        var store = new InMemoryDistributedCircuitStateStore();
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            DateTimeOffset.UtcNow.AddSeconds(1),
            DateTimeOffset.UtcNow,
            null,
            null,
            "e");
        (await store.TryUpdateAsync("k", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();
        await store.PublishHealthAsync("k", new DistributedHealthContribution("a", 1, 0, 0, DateTimeOffset.UtcNow), CancellationToken.None);

        await store.ClearHealthAsync("k", CancellationToken.None);
        var afterClearHealth = await store.GetAggregatedHealthAsync("k", DateTimeOffset.UtcNow, TimeSpan.FromMinutes(1), CancellationToken.None);
        afterClearHealth.ContributingInstances.ShouldBe(0);

        store.Clear();
        (await store.GetAsync("k", CancellationToken.None)).ShouldBeNull();
    }

    [Fact]
    public async Task MaxTrackedCircuitKeys_BlocksUnboundedGrowth()
    {
        var store = new InMemoryDistributedCircuitStateStore(maxTrackedCircuitKeys: 1);
        store.MaxTrackedCircuitKeys.ShouldBe(1);

        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            DateTimeOffset.UtcNow.AddSeconds(1),
            DateTimeOffset.UtcNow,
            null,
            null,
            "e");
        (await store.TryUpdateAsync("k1", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();

        var open2 = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            DateTimeOffset.UtcNow.AddSeconds(1),
            DateTimeOffset.UtcNow,
            null,
            null,
            "e");
        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await store.TryUpdateAsync("k2", DistributedCircuitSnapshot.Closed, open2, CancellationToken.None));
    }

    [Fact]
    public async Task MaxTrackedCircuitKeys_CountsHealthOnlyKeys()
    {
        var store = new InMemoryDistributedCircuitStateStore(maxTrackedCircuitKeys: 1);
        await store.PublishHealthAsync(
            "health-only",
            new DistributedHealthContribution("a", 1, 0, 0, DateTimeOffset.UtcNow),
            CancellationToken.None);

        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            DateTimeOffset.UtcNow.AddSeconds(1),
            DateTimeOffset.UtcNow,
            null,
            null,
            "e");
        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await store.TryUpdateAsync("other", DistributedCircuitSnapshot.Closed, open, CancellationToken.None));
    }

    [Fact]
    public void Constructor_InvalidMaxKeys_Throws()
    {
        Should.Throw<ArgumentOutOfRangeException>(() => new InMemoryDistributedCircuitStateStore(0));
    }

    [Fact]
    public async Task GetAggregatedHealth_SaturatesOnOverflow()
    {
        var store = new InMemoryDistributedCircuitStateStore();
        var now = DateTimeOffset.UtcNow;
        await store.PublishHealthAsync("k", new DistributedHealthContribution("a", int.MaxValue, int.MaxValue, 1, now), CancellationToken.None);
        await store.PublishHealthAsync("k", new DistributedHealthContribution("b", int.MaxValue, int.MaxValue, 2, now), CancellationToken.None);

        var aggregate = await store.GetAggregatedHealthAsync("k", now, TimeSpan.FromMinutes(1), CancellationToken.None);
        aggregate.SuccessCount.ShouldBe(int.MaxValue);
        aggregate.FailureCount.ShouldBe(int.MaxValue);
        aggregate.ContributingInstances.ShouldBe(2);
    }

    [Fact]
    public void DistributedTypes_ExposeComputedProperties()
    {
        var contribution = new DistributedHealthContribution("id", 3, 2, 1, DateTimeOffset.UtcNow);
        contribution.Throughput.ShouldBe(5);

        Should.Throw<ArgumentNullException>(() => new DistributedHealthContribution(null!, 0, 0, 0, DateTimeOffset.UtcNow));

        var snapshot = new DistributedCircuitSnapshot(
            CircuitState.HalfOpen,
            2,
            DateTimeOffset.UtcNow.AddSeconds(1),
            DateTimeOffset.UtcNow,
            "owner",
            DateTimeOffset.UtcNow.AddSeconds(2),
            "err");
        snapshot.HalfOpenLeaseExpiresUtc.ShouldNotBeNull();
        snapshot.LastError.ShouldBe("err");

        var empty = new DistributedHealthAggregate(0, 0, 0, 0);
        empty.FailureRate.ShouldBe(0);
        empty.Throughput.ShouldBe(0);

        var nonEmpty = new DistributedHealthAggregate(2, 2, 1, 1);
        nonEmpty.FailureRate.ShouldBe(0.5);
        nonEmpty.Throughput.ShouldBe(4);
    }
}
