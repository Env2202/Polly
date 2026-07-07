namespace Polly.Adaptive;

internal static class AdaptiveConstants
{
    public const string SelfTuningTimeoutDefaultName = "SelfTuningTimeout";

    public const string SelfTuningRetryDefaultName = "SelfTuningRetry";

    public const string OnTimeoutEvent = "OnTimeout";

    public const string OnRetryEvent = "OnRetry";

    public const string OnTimeoutAdjustedEvent = "OnTimeoutAdjusted";

    public const string OnRetryAdjustedEvent = "OnRetryAdjusted";

    public const int DefaultCapacity = 256;

    public const int DefaultMinimumSamples = 20;

    public const double DefaultLatencyPercentile = 0.99;

    public const double DefaultTimeoutMultiplier = 2.0;

    public const int DefaultInitialRetryAttempts = 3;

    public const int DefaultMinRetryAttempts = 1;

    public const int DefaultMaxRetryAttempts = 5;

    public const double DefaultHighFailureRateThreshold = 0.5;

    public static readonly TimeSpan DefaultSamplingWindow = TimeSpan.FromSeconds(30);

    public static readonly TimeSpan DefaultInitialTimeout = TimeSpan.FromSeconds(30);

    public static readonly TimeSpan DefaultMinTimeout = TimeSpan.FromMilliseconds(50);

    public static readonly TimeSpan DefaultMaxTimeout = TimeSpan.FromSeconds(60);

    public static readonly TimeSpan DefaultInitialDelay = TimeSpan.FromSeconds(1);

    public static readonly TimeSpan DefaultMinDelay = TimeSpan.FromMilliseconds(100);

    public static readonly TimeSpan DefaultMaxDelay = TimeSpan.FromSeconds(30);
}
