namespace Api.Workers;

using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using Amazon.SQS;
using Amazon.SQS.Model;
using Api.Configuration;
using Api.Domain.Events;
using Api.Infrastructure;
using Api.Infrastructure.Queue;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

public sealed class EventProcessorWorker(
    ChannelReader<InventoryEventEnvelope> channelReader,
    IAmazonSQS sqsClient,
    IOptions<SqsOptions> sqsOptions,
    ILogger<EventProcessorWorker> logger) : BackgroundService
{
  private readonly SqsOptions _sqsOptions = sqsOptions.Value;

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    logger.LogInformation("Event Processor worker started.");

    await foreach (var envelope in channelReader.ReadAllAsync(stoppingToken))
    {
      await ProcessEnvelopeAsync(envelope, stoppingToken);
    }

    logger.LogInformation("Event Processor worker drained and completed.");
  }

  private async Task ProcessEnvelopeAsync(InventoryEventEnvelope envelope, CancellationToken cancellationToken)
  {
    // Rehydrate distributed trace context and link span
    using var activity = Diagnostics.ActivitySource.StartActivity(
        "inventory.process",
        ActivityKind.Consumer,
        envelope.PropagationContext.ActivityContext
    );

    activity?.SetTag("messaging.system", "sqs");
    activity?.SetTag("messaging.message_id", envelope.MessageId);
    activity?.SetTag("messaging.event_type", envelope.EventType);

    try
    {
      switch (envelope.EventType)
      {
        case nameof(InventorySnapshotUpdated):
          var snapshot = JsonSerializer.Deserialize<InventorySnapshotUpdated>(envelope.Body);
          logger.LogInformation(
              "Processed snapshot for property {PropertyId}, unit {UnitTypeId} ({Count} dates).",
              snapshot?.PropertyId,
              snapshot?.UnitTypeId,
              snapshot?.Counts.Count
          );
          break;

        case nameof(InventoryDeltaAdjusted):
          var delta = JsonSerializer.Deserialize<InventoryDeltaAdjusted>(envelope.Body);
          logger.LogInformation(
              "Processed delta adjustment for property {PropertyId}, unit {UnitTypeId}, Delta: {Delta}.",
              delta?.PropertyId,
              delta?.UnitTypeId,
              delta?.Adjustments.Count
          );
          break;

        default:
          logger.LogWarning("Unrecognized event type {EventType}. Skipping dispatch.", envelope.EventType);
          break;
      }

      // Acknowledge and remove from queue
      await sqsClient.DeleteMessageAsync(new DeleteMessageRequest
      {
        QueueUrl = _sqsOptions.QueueUrl,
        ReceiptHandle = envelope.ReceiptHandle
      }, cancellationToken);
    }
    catch (Exception ex)
    {
      logger.LogError(ex, "Failed to process message {MessageId}. Leaving on queue for redelivery.", envelope.MessageId);
    }
  }
}
