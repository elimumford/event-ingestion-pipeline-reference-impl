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
              await HandleSnapshotAsync(dbContext, snapshot, logger, cancellationToken);
            }
            break;

          case nameof(InventoryDeltaAdjusted):
            var delta = JsonSerializer.Deserialize<InventoryDeltaAdjusted>(envelope.Body);
            if (delta is not null)
            {
              await HandleDeltaAsync(dbContext, delta, logger, cancellationToken);
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
      InventoryDbContext db,
      InventorySnapshotUpdated payload,
      ILogger? logger = null,
      CancellationToken ct = default)
  {
    foreach (var count in payload.Counts)
    {
      var existing = await db.InventoryItems
          .FirstOrDefaultAsync(x =>
              x.PropertyId == payload.PropertyId &&
              x.UnitTypeId == payload.UnitTypeId &&
              x.Date == count.Date, ct);

      if (existing is null)
      {
        db.InventoryItems.Add(new InventoryItem
        {
          PropertyId = payload.PropertyId,
          UnitTypeId = payload.UnitTypeId,
          Date = count.Date,
          AvailableInventory = count.AvailableInventory,
          LastSnapshotUtc = payload.Timestamp,
          RecentEventIds = [payload.EventId]
        });
      }
      else
      {
        // Drop stale snapshots that arrived out of order
        if (payload.Timestamp < existing.LastSnapshotUtc)
        {
          logger?.LogWarning(
              "Stale snapshot {EventId} for {Date} ignored. Snapshot timestamp {Timestamp} predates baseline {Baseline}",
              payload.EventId, count.Date, payload.Timestamp, existing.LastSnapshotUtc);
          continue;
        }

        existing.AvailableInventory = count.AvailableInventory;
        existing.LastSnapshotUtc = payload.Timestamp;
        // Snapshot establishes a new baseline; reset buffer to start clean from this event
        existing.RecentEventIds = [payload.EventId];
      }
    }

    await db.SaveChangesAsync(ct);
  }

  internal static async Task HandleDeltaAsync(
      InventoryDbContext db,
      InventoryDeltaAdjusted payload,
      ILogger? logger = null,
      CancellationToken ct = default)
  {
    foreach (var adj in payload.Adjustments)
    {
      var existing = await db.InventoryItems
          .FirstOrDefaultAsync(x =>
              x.PropertyId == payload.PropertyId &&
              x.UnitTypeId == payload.UnitTypeId &&
              x.Date == adj.Date, ct);

      if (existing is null)
      {
        // First time seeing this date: drop if predated, otherwise initialize with clamp
        var initialCount = Math.Max(0, adj.AvailableInventoryAdjustment);
        db.InventoryItems.Add(new InventoryItem
        {
          PropertyId = payload.PropertyId,
          UnitTypeId = payload.UnitTypeId,
          Date = adj.Date,
          AvailableInventory = initialCount,
          LastSnapshotUtc = DateTimeOffset.MinValue,
          RecentEventIds = [payload.EventId]
        });
        continue;
      }

      // 1. Snapshot epoch check: discard deltas predating the authoritative baseline
      if (payload.Timestamp < existing.LastSnapshotUtc)
      {
        logger?.LogWarning(
            "Delta {EventId} for {Date} predates last snapshot baseline ({Baseline}). Skipping.",
            payload.EventId, adj.Date, existing.LastSnapshotUtc);
        continue;
      }

      // 2. Replay check: discard duplicate redeliveries
      if (existing.RecentEventIds.Contains(payload.EventId))
      {
        logger?.LogInformation(
            "Duplicate delta {EventId} detected for {Date}. Skipping.",
            payload.EventId, adj.Date);
        continue;
      }

      // 3. Apply adjustment with zero clamping
      existing.AvailableInventory = Math.Max(0, existing.AvailableInventory + adj.AvailableInventoryAdjustment);

      // 4. Update bounded ring buffer (reassign expression to ensure EF Core tracks mutation)
      existing.RecentEventIds = [.. existing.RecentEventIds.TakeLast(19), payload.EventId];
    }

    await db.SaveChangesAsync(ct);
  }
}
