namespace Api.Infrastructure.HealthChecks;

using Amazon.SQS;
using Amazon.SQS.Model;
using Api.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

public sealed class SqsHealthCheck(IAmazonSQS sqs, IOptions<SqsOptions> options) : IHealthCheck
{
  public async Task<HealthCheckResult> CheckHealthAsync(
      HealthCheckContext context, CancellationToken cancellationToken = default)
  {
    await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
    {
      QueueUrl = options.Value.QueueUrl,
      AttributeNames = ["QueueArn"]
    }, cancellationToken);
    return HealthCheckResult.Healthy();
  }
}
