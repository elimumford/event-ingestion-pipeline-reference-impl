namespace Api.Workers;

using System.Threading.Channels;
using Amazon.SQS;
using Amazon.SQS.Model;
using Api.Configuration;
using Api.Infrastructure.Queue;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenTelemetry.Context.Propagation;

public sealed class SqsConsumerWorker(
    IAmazonSQS sqsClient,
    IOptions<SqsOptions> sqsOptions,
    ChannelWriter<InventoryEventEnvelope> channelWriter,
    ILogger<SqsConsumerWorker> logger) : BackgroundService
{
  private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;
  private readonly SqsOptions _sqsOptions = sqsOptions.Value;

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    logger.LogInformation("SQS Poller worker started. Polling queue: {QueueUrl}", _sqsOptions.QueueUrl);

    var request = new ReceiveMessageRequest
    {
      QueueUrl = _sqsOptions.QueueUrl,
      MaxNumberOfMessages = 10,
      WaitTimeSeconds = 20, // SQS Long Polling
      MessageAttributeNames = ["All"]
    };

    while (!stoppingToken.IsCancellationRequested)
    {
      try
      {
        var response = await sqsClient.ReceiveMessageAsync(request, stoppingToken);

        if (response.Messages.Count == 0)
        {
          continue;
        }

        foreach (var message in response.Messages)
        {
          // Extract W3C trace context from SQS attributes
          var propagationContext = Propagator.Extract(
              default,
              message.MessageAttributes,
              static (attributes, key) =>
              {
                return attributes.TryGetValue(key, out var attr) && !string.IsNullOrWhiteSpace(attr.StringValue)
                              ? [attr.StringValue]
                              : [];
              }
          );

          var eventType = message.MessageAttributes.TryGetValue("EventType", out var typeAttr)
              ? typeAttr.StringValue
              : "Unknown";

          var envelope = new InventoryEventEnvelope(
              message.MessageId,
              message.ReceiptHandle,
              eventType,
              message.Body,
              propagationContext
          );

          // Push to channel applying backpressure if full
          await channelWriter.WriteAsync(envelope, stoppingToken);
        }
      }
      catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
      {
        break;
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Transient error occurred while polling SQS. Backing off 2 seconds.");
        await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
      }
    }

    channelWriter.Complete();
    logger.LogInformation("SQS Poller worker stopped cleanly.");
  }
}
