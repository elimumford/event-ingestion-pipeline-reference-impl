namespace Api.Workers;

using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using Amazon.SQS;
using Amazon.SQS.Model;
using Api.Configuration;
using Api.Domain.Entities;
using Api.Domain.Events;
using Api.Infrastructure;
using Api.Infrastructure.Persistence;
using Api.Infrastructure.Queue;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

public sealed class EventProcessorWorker(
    ChannelReader<InventoryEventEnvelope> channelReader,
    IAmazonSQS sqsClient,
    IOptions<SqsOptions> sqsOptions,
    IServiceScopeFactory scopeFactory,
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
      using (var scope = scopeFactory.CreateScope())
      {
        var dbContext = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();

        switch (envelope.EventType)
        {
          case nameof(InventorySnapshotUpdated):
            var snapshot = JsonSerializer.Deserialize<InventorySnapshotUpdated>(envelope.Body);
            if (snapshot is not null)
            {
              await HandleSnapshotAsync(dbContext, snapshot, cancellationToken);
            }
            break;

          case nameof(InventoryDeltaAdjusted):
            var delta = JsonSerializer.Deserialize<InventoryDeltaAdjusted>(envelope.Body);
            if (delta is not null)
            {
              await HandleDeltaAsync(dbContext, delta, cancellationToken);
            }
            break;

          default:
            logger.LogWarning("Unrecognized event type {EventType}. Skipping dispatch.", envelope.EventType);
            break;
        }

        await dbContext.SaveChangesAsync(cancellationToken);
      }

      // Acknowledge and delete message only after persistence commits
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

  internal static async Task HandleSnapshotAsync(
      InventoryDbContext dbContext,
      InventorySnapshotUpdated snapshot,
      CancellationToken cancellationToken)
  {
    var dates = snapshot.Counts.Select(c => c.Date).ToArray();

    var existingItems = await dbContext.InventoryItems
        .Where(i => i.PropertyId == snapshot.PropertyId &&
                    i.UnitTypeId == snapshot.UnitTypeId &&
                    dates.Contains(i.Date))
        .ToDictionaryAsync(i => i.Date, cancellationToken);

    foreach (var count in snapshot.Counts)
    {
      if (existingItems.TryGetValue(count.Date, out var existing))
      {
        existing.AvailableInventory = count.AvailableInventory;
      }
      else
      {
        dbContext.InventoryItems.Add(new InventoryItem
        {
          PropertyId = snapshot.PropertyId,
          UnitTypeId = snapshot.UnitTypeId,
          Date = count.Date,
          AvailableInventory = count.AvailableInventory
        });
      }
    }
  }

  internal static async Task HandleDeltaAsync(
      InventoryDbContext dbContext,
      InventoryDeltaAdjusted delta,
      CancellationToken cancellationToken)
  {
    var dates = delta.Adjustments.Select(a => a.Date).ToArray();

    var existingItems = await dbContext.InventoryItems
        .Where(i => i.PropertyId == delta.PropertyId &&
                    i.UnitTypeId == delta.UnitTypeId &&
                    dates.Contains(i.Date))
        .ToDictionaryAsync(i => i.Date, cancellationToken);

    foreach (var adjustment in delta.Adjustments)
    {
      if (existingItems.TryGetValue(adjustment.Date, out var existing))
      {
        // Apply relative adjustment to existing baseline
        existing.AvailableInventory += adjustment.AvailableInventoryAdjustment;

        // Ensure inventory does not go below zero
        if (existing.AvailableInventory < 0)
        {
          existing.AvailableInventory = 0;
        }
      }
      else
      {
        dbContext.InventoryItems.Add(new InventoryItem
        {
          PropertyId = delta.PropertyId,
          UnitTypeId = delta.UnitTypeId,
          Date = adjustment.Date,
          AvailableInventory = Math.Max(0, adjustment.AvailableInventoryAdjustment)
        });
      }
    }
  }
}
