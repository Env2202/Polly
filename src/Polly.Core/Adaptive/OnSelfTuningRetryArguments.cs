using Polly.Utils;

namespace Polly.Adaptive;

#pragma warning disable CA1815 // Override equals and operator equals on value types

/// <summary>
/// Arguments used when a self-tuning retry is about to delay and re-execute.
/// </summary>
/// <typeparam name="TResult">The type of result the strategy handles.</typeparam>
/// <remarks>
/// Always use the constructor when creating this struct, otherwise we do not guarantee binary compatibility.
/// </remarks>
public readonly struct OnSelfTuningRetryArguments<TResult> : IOutcomeArguments<TResult>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="OnSelfTuningRetryArguments{TResult}"/> struct.
    /// </summary>
    /// <param name="context">The resilience context.</param>
    /// <param name="outcome">The outcome that triggered the retry.</param>
    /// <param name="attemptNumber">Zero-based attempt number that failed.</param>
    /// <param name="retryDelay">Delay that will be applied before the next attempt.</param>
    /// <param name="maxRetryAttempts">Adaptive max retry attempts used for this execution.</param>
    /// <param name="duration">Duration of the failed attempt.</param>
    public OnSelfTuningRetryArguments(
        ResilienceContext context,
        Outcome<TResult> outcome,
        int attemptNumber,
        TimeSpan retryDelay,
        int maxRetryAttempts,
        TimeSpan duration)
    {
        Context = context;
        Outcome = outcome;
        AttemptNumber = attemptNumber;
        RetryDelay = retryDelay;
        MaxRetryAttempts = maxRetryAttempts;
        Duration = duration;
    }

    /// <summary>
    /// Gets the resilience context.
    /// </summary>
    public ResilienceContext Context { get; }

    /// <summary>
    /// Gets the outcome that triggered the retry.
    /// </summary>
    public Outcome<TResult> Outcome { get; }

    /// <summary>
    /// Gets the zero-based attempt number that just failed.
    /// </summary>
    public int AttemptNumber { get; }

    /// <summary>
    /// Gets the delay that will be applied before the next attempt.
    /// </summary>
    public TimeSpan RetryDelay { get; }

    /// <summary>
    /// Gets the adaptive maximum number of retry attempts for this execution.
    /// </summary>
    public int MaxRetryAttempts { get; }

    /// <summary>
    /// Gets the duration of the failed attempt.
    /// </summary>
    public TimeSpan Duration { get; }
}
