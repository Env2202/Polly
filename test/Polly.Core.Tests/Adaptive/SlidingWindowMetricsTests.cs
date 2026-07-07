using Microsoft.Extensions.Time.Testing;
using Polly.Adaptive;

namespace Polly.Core.Tests.Adaptive;

public class SlidingWindowMetricsTests
{
    [Fact]
    public void Ctor_InvalidArgs_Throws()
    {
        Should.Throw<ArgumentOutOfRangeException>(() => new SlidingWindowMetrics(TimeSpan.Zero, 10));
        Should.Throw<ArgumentOutOfRangeException>(() => new SlidingWindowMetrics(TimeSpan.FromSeconds(1), 0));
    }

    [Fact]
    public void Record_AndSnapshot_TracksSuccessAndFailure()
    {
        var time = new FakeTimeProvider();
        var metrics = new SlidingWindowMetrics(TimeSpan.FromSeconds(30), 100, time);

        metrics.Record(TimeSpan.FromMilliseconds(10), success: true);
        metrics.Record(TimeSpan.FromMilliseconds(20), success: true);
        metrics.Record(TimeSpan.FromMilliseconds(30), success: false);

        var snapshot = metrics.GetSnapshot();
        snapshot.SampleCount.ShouldBe(3);
        snapshot.SuccessCount.ShouldBe(2);
        snapshot.FailureCount.ShouldBe(1);
        snapshot.SuccessRate.ShouldBe(2.0 / 3.0, 0.0001);
        snapshot.FailureRate.ShouldBe(1.0 / 3.0, 0.0001);
        snapshot.MinDuration.ShouldBe(TimeSpan.FromMilliseconds(10));
        snapshot.MaxDuration.ShouldBe(TimeSpan.FromMilliseconds(30));
        snapshot.AverageDuration.ShouldBe(TimeSpan.FromMilliseconds(20));
    }

    [Fact]
    public void GetLatencyPercentile_Empty_ReturnsZero()
    {
        var metrics = new SlidingWindowMetrics(TimeSpan.FromSeconds(30), 10);
        metrics.GetLatencyPercentile(0.99).ShouldBe(TimeSpan.Zero);
    }

    [Fact]
    public void GetLatencyPercentile_Invalid_Throws()
    {
        var metrics = new SlidingWindowMetrics(TimeSpan.FromSeconds(30), 10);
        Should.Throw<ArgumentOutOfRangeException>(() => metrics.GetLatencyPercentile(-0.1));
        Should.Throw<ArgumentOutOfRangeException>(() => metrics.GetLatencyPercentile(1.1));
    }

    [Fact]
    public void GetLatencyPercentile_ComputesNearestRank()
    {
        var time = new FakeTimeProvider();
        var metrics = new SlidingWindowMetrics(TimeSpan.FromMinutes(1), 100, time);

        for (var i = 1; i <= 100; i++)
        {
            metrics.Record(TimeSpan.FromMilliseconds(i), success: true);
        }

        // P100 -> last (100ms), P0 -> first (1ms), P50 ~ 50ms
        metrics.GetLatencyPercentile(1.0).ShouldBe(TimeSpan.FromMilliseconds(100));
        metrics.GetLatencyPercentile(0.0).ShouldBe(TimeSpan.FromMilliseconds(1));
        metrics.GetLatencyPercentile(0.5).ShouldBe(TimeSpan.FromMilliseconds(50));
    }

    [Fact]
    public void Samples_ExpireAfterSamplingWindow()
    {
        var time = new FakeTimeProvider();
        var metrics = new SlidingWindowMetrics(TimeSpan.FromSeconds(10), 100, time);

        metrics.Record(TimeSpan.FromMilliseconds(5), success: true);
        metrics.GetSnapshot().SampleCount.ShouldBe(1);

        time.Advance(TimeSpan.FromSeconds(11));
        metrics.GetSnapshot().SampleCount.ShouldBe(0);
    }

    [Fact]
    public void Capacity_OverwritesOldest()
    {
        var time = new FakeTimeProvider();
        var metrics = new SlidingWindowMetrics(TimeSpan.FromMinutes(5), capacity: 3, time);

        metrics.Record(TimeSpan.FromMilliseconds(1), true);
        metrics.Record(TimeSpan.FromMilliseconds(2), true);
        metrics.Record(TimeSpan.FromMilliseconds(3), true);
        metrics.Record(TimeSpan.FromMilliseconds(4), true);

        var snapshot = metrics.GetSnapshot();
        snapshot.SampleCount.ShouldBe(3);
        snapshot.MinDuration.ShouldBe(TimeSpan.FromMilliseconds(2));
        snapshot.MaxDuration.ShouldBe(TimeSpan.FromMilliseconds(4));
    }

    [Fact]
    public void Reset_ClearsWindow()
    {
        var metrics = new SlidingWindowMetrics(TimeSpan.FromSeconds(30), 10);
        metrics.Record(TimeSpan.FromMilliseconds(1), true);
        metrics.Reset();
        metrics.GetSnapshot().SampleCount.ShouldBe(0);
    }

    [Fact]
    public void ConcurrentRecord_IsThreadSafe()
    {
        var metrics = new SlidingWindowMetrics(TimeSpan.FromMinutes(1), 10_000);
        const int threads = 8;
        const int perThread = 500;

        Parallel.For(0, threads, _ =>
        {
            for (var i = 0; i < perThread; i++)
            {
                metrics.Record(TimeSpan.FromMilliseconds(i % 50), success: i % 2 == 0);
            }
        });

        var snapshot = metrics.GetSnapshot();
        snapshot.SampleCount.ShouldBe(threads * perThread);
        (snapshot.SuccessCount + snapshot.FailureCount).ShouldBe(snapshot.SampleCount);
    }
}
