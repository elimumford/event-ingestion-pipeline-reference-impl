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
    IInventoryConflictClassifier conflictClassifier,
    ILogger<EventProcessorWorker> logger) : BackgroundService
{
  private const int MaxConcurrencyAttempts = 4;
  private readonly SqsOptions _sqsOptions = sqsOptions.Value;

  public override async Task StopAsync(CancellationToken cancellationToken)
  {
    try
    {
      if (ExecuteTask is { } execution)
      {
        await execution.WaitAsync(cancellationToken);
      }
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
      logger.LogWarning("Shutdown deadline reached. Cancelling inventory processing; unfinished messages will be redelivered.");
    }
    finally
    {
      await base.StopAsync(cancellationToken);
    }
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    logger.LogInformation("Event Processor worker started.");

    try
    {
      await foreach (var envelope in channelReader.ReadAllAsync(stoppingToken))
      {
        await ProcessEnvelopeAsync(envelope, stoppingToken);
        stoppingToken.ThrowIfCancellationRequested();
      }
    }
    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
    {
      logger.LogWarning("Event Processor worker cancelled before the channel was fully drained.");
      return;
    }
    finally
    {
      while (channelReader.TryRead(out var pending))
      {
        if (pending.VisibilityLease is { } lease)
        {
          await lease.DisposeAsync();
        }
      }
    }

    logger.LogInformation("Event Processor worker drained and completed.");
  }

  private async Task ProcessEnvelopeAsync(InventoryEventEnvelope envelope, CancellationToken cancellationToken)
  {
    await using var lease = envelope.VisibilityLease;
    using var attemptCancellation = lease is null
        ? null
        : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, lease.LeaseLostToken);
    cancellationToken = attemptCancellation?.Token ?? cancellationToken;

    using var activity = Diagnostics.ActivitySource.StartActivity(
        "inventory.process",
        ActivityKind.Consumer,
        envelope.PropagationContext.ActivityContext
    );

    activity?.SetTag("messaging.system", "sqs");
    activity?.SetTag("messaging.message_id", envelope.MessageId);
    activity?.SetTag("messaging.event_type", envelope.EventType);

    for (var attempt = 1; attempt <= MaxConcurrencyAttempts; attempt++)
    {
      try
      {
        cancellationToken.ThrowIfCancellationRequested();
        await ProcessEnvelopeAttemptAsync(envelope, cancellationToken);

        if (lease is not null)
        {
          await lease.StopRenewalAsync();
        }
        cancellationToken.ThrowIfCancellationRequested();
        await sqsClient.DeleteMessageAsync(new DeleteMessageRequest
        {
          QueueUrl = _sqsOptions.QueueUrl,
          ReceiptHandle = envelope.ReceiptHandle
        }, cancellationToken);

        return;
      }
      catch (Exception ex) when (attempt < MaxConcurrencyAttempts && conflictClassifier.IsRetryableConflict(ex))
      {
        logger.LogWarning(
            ex,
            "Concurrent inventory update for message {MessageId}; retrying attempt {Attempt}/{MaxAttempts}.",
            envelope.MessageId,
            attempt + 1,
            MaxConcurrencyAttempts);

        try
        {
          await Task.Delay(TimeSpan.FromMilliseconds(attempt * 25), cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
          return;
        }
      }
      catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
      {
        logger.LogWarning("Processing message {MessageId} cancelled. Leaving on queue for redelivery.", envelope.MessageId);
        return;
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Failed to process message {MessageId}. Leaving on queue for redelivery.", envelope.MessageId);
        return;
      }
    }
  }

  private async Task ProcessEnvelopeAttemptAsync(
      InventoryEventEnvelope envelope,
      CancellationToken cancellationToken)
  {
    using var scope = scopeFactory.CreateScope();
    var dbContext = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();

    switch (envelope.EventType)
    {
      case nameof(InventorySnapshotUpdated):
        var snapshot = JsonSerializer.Deserialize<InventorySnapshotUpdated>(envelope.Body)
            ?? throw new JsonException("Inventory snapshot payload cannot be null.");
        await HandleSnapshotAsync(dbContext, snapshot, logger, cancellationToken);
        break;

      case nameof(InventoryDeltaAdjusted):
        var delta = JsonSerializer.Deserialize<InventoryDeltaAdjusted>(envelope.Body)
            ?? throw new JsonException("Inventory delta payload cannot be null.");
        await HandleDeltaAsync(dbContext, delta, logger, cancellationToken);
        break;

      default:
        throw new NotSupportedException($"Inventory event type '{envelope.EventType}' is not supported.");
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
        // A snapshot must advance the baseline; equal timestamps have no reliable ordering.
        if (payload.Timestamp <= existing.LastSnapshotUtc)
        {
          logger?.LogWarning(
              "Snapshot {EventId} for {Date} ignored. Timestamp {Timestamp} does not advance baseline {Baseline}",
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
