namespace Polly.Adaptive;

#pragma warning disable CA1815 // Override equals and operator equals on value types

/// <summary>
/// Arguments used when a self-tuning timeout fires.
/// </summary>
/// <remarks>
/// Always use the constructor when creating this struct, otherwise we do not guarantee binary compatibility.
/// </remarks>
public readonly struct OnSelfTuningTimeoutArguments
{
    /// <summary>
    /// Initializes a new instance of the <see cref="OnSelfTuningTimeoutArguments"/> struct.
    /// </summary>
    /// <param name="context">The resilience context.</param>
    /// <param name="timeout">The adaptive timeout that was applied.</param>
    public OnSelfTuningTimeoutArguments(ResilienceContext context, TimeSpan timeout)
    {
        Context = context;
        Timeout = timeout;
    }

    /// <summary>
    /// Gets the context associated with the execution.
    /// </summary>
    public ResilienceContext Context { get; }

    /// <summary>
    /// Gets the adaptive timeout that was applied.
    /// </summary>
    public TimeSpan Timeout { get; }
}
