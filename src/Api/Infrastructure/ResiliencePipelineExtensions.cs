namespace Api.Infrastructure;

using System;
using Amazon.SQS;
using Microsoft.Extensions.DependencyInjection;
using Polly;
using Polly.Retry;

public static class ResiliencePipelineExtensions
{
  public const string SqsPipelineKey = "sqs-resilience-pipeline";

  public static IServiceCollection AddCustomResiliencePipelines(this IServiceCollection services)
  {
    services.AddResiliencePipeline(SqsPipelineKey, builder =>
    {
      builder.AddRetry(new RetryStrategyOptions
      {
        MaxRetryAttempts = 3,
        BackoffType = DelayBackoffType.Exponential,
        UseJitter = true,
        Delay = TimeSpan.FromMilliseconds(100),
        ShouldHandle = new PredicateBuilder().Handle<AmazonSQSException>()
      });
    });

    return services;
  }
}
