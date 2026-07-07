using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;
using Polly.Adaptive;

namespace Polly;

/// <summary>
/// Extensions for adding self-tuning timeout strategies to <see cref="ResiliencePipelineBuilder"/>.
/// </summary>
public static class SelfTuningTimeoutResiliencePipelineBuilderExtensions
{
    /// <summary>
    /// Adds a self-tuning timeout strategy that adapts the timeout from recent observed latencies.
    /// </summary>
    /// <typeparam name="TBuilder">The builder type.</typeparam>
    /// <param name="builder">The builder instance.</param>
    /// <param name="options">The strategy options.</param>
    /// <returns>The same builder instance.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> or <paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ValidationException">Thrown when <paramref name="options"/> are invalid.</exception>
    [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(SelfTuningTimeoutStrategyOptions))]
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026:Members annotated with 'RequiresUnreferencedCodeAttribute' require dynamic access otherwise can break functionality when trimming application code",
        Justification = "All options members preserved.")]
    public static TBuilder AddSelfTuningTimeout<TBuilder>(this TBuilder builder, SelfTuningTimeoutStrategyOptions options)
        where TBuilder : ResiliencePipelineBuilderBase
    {
        Guard.NotNull(builder);
        Guard.NotNull(options);

        if (options.MinTimeout > options.MaxTimeout)
        {
            throw new ValidationException($"{nameof(options.MinTimeout)} must be less than or equal to {nameof(options.MaxTimeout)}.");
        }

        builder.AddStrategy(
            context => new SelfTuningTimeoutResilienceStrategy(options, context.TimeProvider, context.Telemetry),
            options);

        return builder;
    }
}
