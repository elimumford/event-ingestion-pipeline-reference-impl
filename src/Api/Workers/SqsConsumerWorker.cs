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
  private readonly CancellationTokenSource _abortWrites = new();

  public override async Task StopAsync(CancellationToken cancellationToken)
  {
    using var deadline = cancellationToken.Register(() => _abortWrites.Cancel());
    await base.StopAsync(cancellationToken);
  }

  public override void Dispose()
  {
    _abortWrites.Cancel();
    base.Dispose();
    _abortWrites.Dispose();
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    var drainToken = _abortWrites.Token;
    logger.LogInformation("SQS Poller worker started. Polling queue: {QueueUrl}", _sqsOptions.QueueUrl);

    var request = new ReceiveMessageRequest
    {
      QueueUrl = _sqsOptions.QueueUrl,
      MaxNumberOfMessages = 10,
      WaitTimeSeconds = 20, // SQS Long Polling
      VisibilityTimeout = _sqsOptions.VisibilityTimeoutSeconds,
      MessageAttributeNames = ["All"]
    };

    try
    {
      while (!stoppingToken.IsCancellationRequested)
      {
        try
        {
          var response = await sqsClient.ReceiveMessageAsync(request, stoppingToken);

          if (response.Messages is not { Count: > 0 })
          {
            continue;
          }

          // Start every lease before waiting for channel space, including the rest of the batch.
          var envelopes = response.Messages.Select(message =>
          {
            var propagationContext = Propagator.Extract(
                default,
                message.MessageAttributes,
                static (attributes, key) =>
                {
                  return attributes is not null && attributes.TryGetValue(key, out var attr) && !string.IsNullOrWhiteSpace(attr.StringValue)
                                ? [attr.StringValue]
                                : [];
                }
            );

            var eventType = message.MessageAttributes?.TryGetValue("EventType", out var typeAttr) == true
                ? typeAttr.StringValue
                : "Unknown";

            return new InventoryEventEnvelope(
                message.MessageId,
                message.ReceiptHandle,
                eventType,
                message.Body,
                propagationContext
            )
            {
              // Ownership transfers to the processor; polling cancellation must not end the lease.
              VisibilityLease = new SqsVisibilityLease(
                  sqsClient, _sqsOptions.QueueUrl, message.ReceiptHandle,
                  _sqsOptions.VisibilityTimeoutSeconds, logger, CancellationToken.None)
            };
          }).ToArray();

          var written = 0;
          try
          {
            for (; written < envelopes.Length; written++)
            {
              // Finish transferring the received batch unless the host's shutdown budget expires.
              await channelWriter.WriteAsync(envelopes[written], drainToken);
            }
          }
          finally
          {
            for (var i = written; i < envelopes.Length; i++)
            {
              if (envelopes[i].VisibilityLease is { } lease)
              {
                await lease.DisposeAsync();
              }
            }
          }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested || drainToken.IsCancellationRequested)
        {
          break;
        }
        catch (Exception ex)
        {
          logger.LogError(ex, "Transient error occurred while polling SQS. Backing off 2 seconds.");
          await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        }
      }
    }
    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
    {
      logger.LogInformation("SQS polling backoff cancelled for shutdown.");
    }
    finally
    {
      channelWriter.TryComplete();
      logger.LogInformation("SQS Poller worker stopped; channel writer completed.");
    }
  }
}
