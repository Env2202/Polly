namespace Polly.CircuitBreaker.Distributed;

internal static class DistributedCircuitBreakerConstants
{
    public const string DefaultName = "DistributedCircuitBreaker";

    public const string OnOpenedEvent = "OnCircuitOpened";

    public const string OnClosedEvent = "OnCircuitClosed";

    public const string OnHalfOpenEvent = "OnCircuitHalfOpened";

    public static readonly TimeSpan DefaultBreakDuration = TimeSpan.FromSeconds(5);

    public static readonly TimeSpan DefaultSamplingDuration = TimeSpan.FromSeconds(30);

    public static readonly TimeSpan DefaultAllowedClockSkew = TimeSpan.FromSeconds(2);

    public static readonly TimeSpan DefaultHalfOpenLeaseDuration = TimeSpan.FromSeconds(5);

    public static readonly TimeSpan DefaultStateRefreshInterval = TimeSpan.FromMilliseconds(100);

    public const double DefaultFailureRatio = 0.5;

    public const int DefaultMinimumThroughput = 20;
}
