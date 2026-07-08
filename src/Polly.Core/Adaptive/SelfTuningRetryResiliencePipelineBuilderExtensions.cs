using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;
using Polly.Adaptive;

namespace Polly;

/// <summary>
/// Extensions for adding self-tuning retry strategies to <see cref="ResiliencePipelineBuilder"/>.
/// </summary>
public static class SelfTuningRetryResiliencePipelineBuilderExtensions
{
    /// <summary>
    /// Adds a self-tuning retry strategy that adapts retry attempts and delay from recent outcomes.
    /// </summary>
    /// <param name="builder">The builder instance.</param>
    /// <param name="options">The strategy options.</param>
    /// <returns>The builder instance with the strategy added.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> or <paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ValidationException">Thrown when <paramref name="options"/> are invalid.</exception>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026:Members annotated with 'RequiresUnreferencedCodeAttribute' require dynamic access otherwise can break functionality when trimming application code",
        Justification = "All options members preserved.")]
    [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(SelfTuningRetryStrategyOptions))]
    public static ResiliencePipelineBuilder AddSelfTuningRetry(
        this ResiliencePipelineBuilder builder,
        SelfTuningRetryStrategyOptions options)
    {
        Guard.NotNull(builder);
        Guard.NotNull(options);
        Validate(options);

        return builder.AddStrategy(
            context => new SelfTuningRetryResilienceStrategy<object>(options, context.TimeProvider, context.Telemetry),
            options);
    }

    /// <summary>
    /// Adds a self-tuning retry strategy that adapts retry attempts and delay from recent outcomes.
    /// </summary>
    /// <typeparam name="TResult">The type of result the strategy handles.</typeparam>
    /// <param name="builder">The builder instance.</param>
    /// <param name="options">The strategy options.</param>
    /// <returns>The builder instance with the strategy added.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> or <paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ValidationException">Thrown when <paramref name="options"/> are invalid.</exception>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026:Members annotated with 'RequiresUnreferencedCodeAttribute' require dynamic access otherwise can break functionality when trimming application code",
        Justification = "All options members preserved.")]
    public static ResiliencePipelineBuilder<TResult> AddSelfTuningRetry<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TResult>(
        this ResiliencePipelineBuilder<TResult> builder,
        SelfTuningRetryStrategyOptions<TResult> options)
    {
        Guard.NotNull(builder);
        Guard.NotNull(options);
        Validate(options);

        return builder.AddStrategy(
            context => new SelfTuningRetryResilienceStrategy<TResult>(options, context.TimeProvider, context.Telemetry),
            options);
    }

    private static void Validate<TResult>(SelfTuningRetryStrategyOptions<TResult> options)
    {
        if (options.MinRetryAttempts > options.MaxRetryAttempts)
        {
            throw new ValidationException(
                $"{nameof(options.MinRetryAttempts)} must be less than or equal to {nameof(options.MaxRetryAttempts)}.");
        }

        if (options.MinDelay > options.MaxDelay)
        {
            throw new ValidationException(
                $"{nameof(options.MinDelay)} must be less than or equal to {nameof(options.MaxDelay)}.");
        }

        if (options.MinimumSamples > options.Capacity)
        {
            throw new ValidationException(
                $"{nameof(options.MinimumSamples)} must be less than or equal to {nameof(options.Capacity)}.");
        }

        if (options.InitialRetryAttempts < options.MinRetryAttempts
            || options.InitialRetryAttempts > options.MaxRetryAttempts)
        {
            throw new ValidationException(
                $"{nameof(options.InitialRetryAttempts)} must be between {nameof(options.MinRetryAttempts)} and {nameof(options.MaxRetryAttempts)} (inclusive).");
        }

        if (options.InitialDelay < options.MinDelay || options.InitialDelay > options.MaxDelay)
        {
            throw new ValidationException(
                $"{nameof(options.InitialDelay)} must be between {nameof(options.MinDelay)} and {nameof(options.MaxDelay)} (inclusive).");
        }
    }
}
