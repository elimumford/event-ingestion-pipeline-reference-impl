namespace IntegrationTests;

using Api.Domain.Entities;
using Api.Domain.Events;
using Api.Domain.Inventory;
using Api.Infrastructure.Persistence;
using Api.Workers;
using IntegrationTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

public class EventProcessorWorkerTests(CustomWebApplicationFactory factory)
    : IClassFixture<CustomWebApplicationFactory>
{
  private readonly string _propertyId = $"prop-{Guid.NewGuid():N}";
  private readonly TestTimeProvider _timeProvider = new(
      new DateTimeOffset(2026, 11, 16, 12, 0, 0, TimeSpan.Zero));

  private InventoryDbContext CreateDbContext()
  {
    var options = new DbContextOptionsBuilder<InventoryDbContext>()
        .UseNpgsql(factory.ConnectionString)
        .Options;

    return new InventoryDbContext(options, _timeProvider);
  }

  [Theory]
  [InlineData(false)]
  [InlineData(true)]
  public async Task SaveChanges_MaintainsAuditTimestamps_WithoutUpdatingUnchangedRows(bool useAsync)
  {
    await using var db = CreateDbContext();
    var createdAt = _timeProvider.UtcNow;
    var date = new DateOnly(2026, 11, 16);
    db.InventoryItems.AddRange(
        new InventoryItem
        {
          PropertyId = _propertyId,
          UnitTypeId = "standard",
          Date = date,
          AvailableInventory = 10,
          CreatedAtUtc = createdAt.AddYears(-1),
          LastUpdatedUtc = createdAt.AddYears(-1)
        },
        new InventoryItem
        {
          PropertyId = _propertyId,
          UnitTypeId = "standard",
          Date = date.AddDays(1),
          AvailableInventory = 5
        });

    Assert.Equal(2, useAsync ? await db.SaveChangesAsync() : db.SaveChanges());
    db.ChangeTracker.Clear();
    var inserted = await db.InventoryItems.Where(item => item.PropertyId == _propertyId)
        .OrderBy(item => item.Date).ToListAsync();
    Assert.All(inserted, item =>
    {
      Assert.Equal(createdAt, item.CreatedAtUtc);
      Assert.Equal(createdAt, item.LastUpdatedUtc);
    });

    _timeProvider.UtcNow = createdAt.AddHours(1);
    inserted[0].AvailableInventory = 8;
    inserted[0].CreatedAtUtc = createdAt.AddYears(1);
    inserted[0].LastUpdatedUtc = DateTimeOffset.MinValue;
    Assert.Equal(1, useAsync
        ? await db.SaveChangesAsync(acceptAllChangesOnSuccess: true)
        : db.SaveChanges(acceptAllChangesOnSuccess: true));

    db.ChangeTracker.Clear();
    var updated = await db.InventoryItems.Where(item => item.PropertyId == _propertyId)
        .OrderBy(item => item.Date).ToListAsync();
    Assert.Equal(createdAt, updated[0].CreatedAtUtc);
    Assert.Equal(_timeProvider.UtcNow, updated[0].LastUpdatedUtc);
    Assert.Equal(8, updated[0].AvailableInventory);
    Assert.Equal(createdAt, updated[1].CreatedAtUtc);
    Assert.Equal(createdAt, updated[1].LastUpdatedUtc);
    var updatedAt = updated[0].LastUpdatedUtc;

    _timeProvider.UtcNow = createdAt.AddHours(2);
    Assert.Equal(0, useAsync ? await db.SaveChangesAsync() : db.SaveChanges());

    // Changing only creation metadata must not create an audit-only update.
    updated[0].CreatedAtUtc = createdAt.AddYears(2);
    Assert.Equal(0, useAsync ? await db.SaveChangesAsync() : db.SaveChanges());
    db.ChangeTracker.Clear();
    var unchanged = await db.InventoryItems.SingleAsync(
        item => item.PropertyId == _propertyId && item.Date == date);
    Assert.Equal(createdAt, unchanged.CreatedAtUtc);
    Assert.Equal(updatedAt, unchanged.LastUpdatedUtc);
  }

  [Fact]
  public async Task HandleSnapshotAsync_InsertsNewItems_AndUpdatesExistingItems()
  {
    // Arrange
    await using var dbContext = CreateDbContext();
    var existingDate = new DateOnly(2026, 11, 10);
    var newDate = new DateOnly(2026, 11, 11);

    dbContext.InventoryItems.Add(new InventoryItem
    {
      PropertyId = _propertyId,
      UnitTypeId = "unit-deluxe-queen",
      Date = existingDate,
      AvailableInventory = 3,
      LastSnapshotUtc = new DateTimeOffset(2026, 11, 1, 0, 0, 0, TimeSpan.Zero),
      RecentEventIds = ["previous-event"]
    });
    await dbContext.SaveChangesAsync();
    dbContext.ChangeTracker.Clear();
    var originalCreatedAt = _timeProvider.UtcNow;
    _timeProvider.UtcNow = originalCreatedAt.AddHours(1);

    var snapshot = new InventorySnapshotUpdated(
        EventId: Guid.NewGuid().ToString(),
        PropertyId: _propertyId,
        UnitTypeId: "unit-deluxe-queen",
        Counts:
        [
            new NightlyInventory(existingDate, 10),
            new NightlyInventory(newDate, 7)
        ],
        Timestamp: new DateTimeOffset(2026, 11, 10, 12, 0, 0, TimeSpan.Zero)
    );

    // Act
    await EventProcessorWorker.HandleSnapshotAsync(dbContext, snapshot, null, CancellationToken.None);
    dbContext.ChangeTracker.Clear();

    // Assert
    var items = await dbContext.InventoryItems
        .Where(i => i.PropertyId == _propertyId)
        .OrderBy(i => i.Date)
        .ToListAsync();

    Assert.Equal(2, items.Count);
    Assert.Equal(existingDate, items[0].Date);
    Assert.Equal(10, items[0].AvailableInventory);
    Assert.Equal(newDate, items[1].Date);
    Assert.Equal(7, items[1].AvailableInventory);
    Assert.All(items, item =>
    {
      Assert.Equal(snapshot.Timestamp, item.LastSnapshotUtc);
      Assert.Equal([snapshot.EventId], item.RecentEventIds);
      Assert.Equal(_timeProvider.UtcNow, item.LastUpdatedUtc);
    });
    Assert.Equal(originalCreatedAt, items[0].CreatedAtUtc);
    Assert.Equal(_timeProvider.UtcNow, items[1].CreatedAtUtc);
  }

  [Fact]
  public async Task HandleDeltaAsync_AppliesAdjustments_AndClampsNegativeInventoryToZero()
  {
    // Arrange
    await using var dbContext = CreateDbContext();
    var datePositiveDelta = new DateOnly(2026, 11, 10);
    var dateClampedExisting = new DateOnly(2026, 11, 11);
    var dateClampedNew = new DateOnly(2026, 11, 12);
    var datePositiveNew = new DateOnly(2026, 11, 13);

    dbContext.InventoryItems.AddRange(
        new InventoryItem
        {
          PropertyId = _propertyId,
          UnitTypeId = "unit-deluxe-queen",
          Date = datePositiveDelta,
          AvailableInventory = 5
        },
        new InventoryItem
        {
          PropertyId = _propertyId,
          UnitTypeId = "unit-deluxe-queen",
          Date = dateClampedExisting,
          AvailableInventory = 2
        }
    );
    await dbContext.SaveChangesAsync();
    dbContext.ChangeTracker.Clear();

    var delta = new InventoryDeltaAdjusted(
        EventId: Guid.NewGuid().ToString(),
        PropertyId: _propertyId,
        UnitTypeId: "unit-deluxe-queen",
        Adjustments:
        [
            new NightlyInventoryAdjustment(datePositiveDelta, 3),
            new NightlyInventoryAdjustment(dateClampedExisting, -5),
            new NightlyInventoryAdjustment(dateClampedNew, -4),
            new NightlyInventoryAdjustment(datePositiveNew, 6)
        ],
        Timestamp: DateTimeOffset.UtcNow
    );

    // Act
    await EventProcessorWorker.HandleDeltaAsync(dbContext, delta, null, CancellationToken.None);
    dbContext.ChangeTracker.Clear();

    // Assert
    var items = await dbContext.InventoryItems
        .Where(i => i.PropertyId == _propertyId)
        .OrderBy(i => i.Date)
        .ToDictionaryAsync(i => i.Date, i => i.AvailableInventory);

    Assert.Equal(4, items.Count);
    Assert.Equal(8, items[datePositiveDelta]);
    Assert.Equal(0, items[dateClampedExisting]);
    Assert.Equal(0, items[dateClampedNew]);
    Assert.Equal(6, items[datePositiveNew]);
  }

  [Fact]
  public async Task HandleDeltaAsync_DuplicateEventId_IsSuppressedAndNotDoubleCounted()
  {
    // Arrange
    await using var db = CreateDbContext();
    var propertyId = _propertyId;
    var unitTypeId = "deluxe-king";
    var date = new DateOnly(2026, 11, 20);

    // Initial state: 10 units available
    db.InventoryItems.Add(new InventoryItem
    {
      PropertyId = propertyId,
      UnitTypeId = unitTypeId,
      Date = date,
      AvailableInventory = 10,
      LastSnapshotUtc = DateTimeOffset.UtcNow.AddHours(-1),
      RecentEventIds = ["evt-existing"]
    });
    await db.SaveChangesAsync();
    db.ChangeTracker.Clear();

    var deltaPayload = new InventoryDeltaAdjusted(
        EventId: "evt-delta-dup",
        Timestamp: DateTimeOffset.UtcNow,
        PropertyId: propertyId,
        UnitTypeId: unitTypeId,
        Adjustments: [new NightlyInventoryAdjustment(date, -2)]
    );

    // Act: First run applies adjustment
    _timeProvider.UtcNow = _timeProvider.UtcNow.AddHours(1);
    await EventProcessorWorker.HandleDeltaAsync(db, deltaPayload);
    db.ChangeTracker.Clear();
    var firstState = await db.InventoryItems.SingleAsync(x => x.PropertyId == propertyId && x.Date == date);
    Assert.Equal(8, firstState.AvailableInventory);
    Assert.Contains("evt-delta-dup", firstState.RecentEventIds);

    // Act: Second run with same EventId (simulating SQS replay)
    _timeProvider.UtcNow = _timeProvider.UtcNow.AddHours(1);
    await EventProcessorWorker.HandleDeltaAsync(db, deltaPayload);
    db.ChangeTracker.Clear();
    var replayState = await db.InventoryItems.SingleAsync(x => x.PropertyId == propertyId && x.Date == date);

    // Assert: Inventory remains 8 (not decremented to 6)
    Assert.Equal(8, replayState.AvailableInventory);
    Assert.Single(replayState.RecentEventIds, id => id == "evt-delta-dup");
    Assert.Equal(firstState.CreatedAtUtc, replayState.CreatedAtUtc);
    Assert.Equal(firstState.LastUpdatedUtc, replayState.LastUpdatedUtc);
  }

  [Fact]
  public async Task HandleDeltaAsync_DeltaPredatingLastSnapshot_IsDiscarded()
  {
    // Arrange
    await using var db = CreateDbContext();
    var propertyId = _propertyId;
    var unitTypeId = "suite";
    var date = new DateOnly(2026, 11, 21);
    var snapshotTime = DateTimeOffset.UtcNow;

    db.InventoryItems.Add(new InventoryItem
    {
      PropertyId = propertyId,
      UnitTypeId = unitTypeId,
      Date = date,
      AvailableInventory = 5,
      LastSnapshotUtc = snapshotTime,
      RecentEventIds = ["evt-snap"]
    });
    await db.SaveChangesAsync();
    db.ChangeTracker.Clear();

    // Delta created 10 minutes BEFORE the snapshot
    var staleDelta = new InventoryDeltaAdjusted(
        EventId: "evt-stale-delta",
        Timestamp: snapshotTime.AddMinutes(-10),
        PropertyId: propertyId,
        UnitTypeId: unitTypeId,
        Adjustments: [new NightlyInventoryAdjustment(date, +3)]
    );

    // Act
    var seededAt = _timeProvider.UtcNow;
    _timeProvider.UtcNow = seededAt.AddHours(1);
    await EventProcessorWorker.HandleDeltaAsync(db, staleDelta);
    db.ChangeTracker.Clear();
    var result = await db.InventoryItems.SingleAsync(x => x.PropertyId == propertyId && x.Date == date);

    // Assert: Delta is ignored because the snapshot baseline supersedes it
    Assert.Equal(5, result.AvailableInventory);
    Assert.DoesNotContain("evt-stale-delta", result.RecentEventIds);
    Assert.Equal(seededAt, result.CreatedAtUtc);
    Assert.Equal(seededAt, result.LastUpdatedUtc);
  }

  [Fact]
  public async Task HandleDeltaAsync_RingBuffer_CapsAtMaxCapacity()
  {
    // Arrange
    await using var db = CreateDbContext();
    var propertyId = _propertyId;
    var unitTypeId = "standard";
    var date = new DateOnly(2026, 11, 22);

    db.InventoryItems.Add(new InventoryItem
    {
      PropertyId = propertyId,
      UnitTypeId = unitTypeId,
      Date = date,
      AvailableInventory = 100,
      LastSnapshotUtc = DateTimeOffset.UtcNow.AddDays(-1),
      RecentEventIds = Enumerable.Range(1, 20).Select(i => $"evt-initial-{i}").ToList()
    });
    await db.SaveChangesAsync();
    db.ChangeTracker.Clear();

    var newDelta = new InventoryDeltaAdjusted(
        EventId: "evt-newest-21",
        Timestamp: DateTimeOffset.UtcNow,
        PropertyId: propertyId,
        UnitTypeId: unitTypeId,
        Adjustments: [new NightlyInventoryAdjustment(date, +1)]
    );

    // Act
    await EventProcessorWorker.HandleDeltaAsync(db, newDelta);
    db.ChangeTracker.Clear();
    var result = await db.InventoryItems.SingleAsync(x => x.PropertyId == propertyId && x.Date == date);

    // Assert: Buffer size remains 20, oldest entry evicted, newest appended
    Assert.Equal(20, result.RecentEventIds.Count);
    Assert.DoesNotContain("evt-initial-1", result.RecentEventIds);
    Assert.Equal("evt-newest-21", result.RecentEventIds.Last());
    Assert.Equal(Enumerable.Range(2, 19).Select(i => $"evt-initial-{i}").Append(newDelta.EventId),
        result.RecentEventIds);
  }

  [Fact]
  public async Task HandleSnapshotAsync_StaleSnapshot_IsIgnored()
  {
    // Arrange
    await using var db = CreateDbContext();
    var propertyId = _propertyId;
    var unitTypeId = "standard";
    var date = new DateOnly(2026, 11, 23);
    var baselineTime = new DateTimeOffset(2026, 11, 23, 12, 0, 0, TimeSpan.Zero);

    db.InventoryItems.Add(new InventoryItem
    {
      PropertyId = propertyId,
      UnitTypeId = unitTypeId,
      Date = date,
      AvailableInventory = 10,
      LastSnapshotUtc = baselineTime,
      RecentEventIds = ["evt-current-snap"]
    });
    await db.SaveChangesAsync();
    db.ChangeTracker.Clear();

    // Snapshot with older timestamp
    var staleSnapshot = new InventorySnapshotUpdated(
        EventId: "evt-stale-snap",
        Timestamp: baselineTime.AddHours(-2),
        PropertyId: propertyId,
        UnitTypeId: unitTypeId,
        Counts: [new NightlyInventory(date, 25)]
    );

    // Act
    var seededAt = _timeProvider.UtcNow;
    _timeProvider.UtcNow = seededAt.AddHours(1);
    await EventProcessorWorker.HandleSnapshotAsync(db, staleSnapshot);
    db.ChangeTracker.Clear();
    var result = await db.InventoryItems.SingleAsync(x => x.PropertyId == propertyId && x.Date == date);

    // Assert: Stale snapshot did not overwrite current baseline
    Assert.Equal(10, result.AvailableInventory);
    Assert.Equal(baselineTime, result.LastSnapshotUtc);
    Assert.DoesNotContain("evt-stale-snap", result.RecentEventIds);
    Assert.Equal(seededAt, result.CreatedAtUtc);
    Assert.Equal(seededAt, result.LastUpdatedUtc);
  }

  [Fact]
  public async Task HandleSnapshotAsync_EqualTimestampSnapshot_IsIgnoredEvenAfterIdEviction()
  {
    await using var db = CreateDbContext();
    var propertyId = _propertyId;
    var unitTypeId = "standard";
    var date = new DateOnly(2026, 11, 24);
    var baselineTime = new DateTimeOffset(2026, 11, 24, 12, 0, 0, TimeSpan.Zero);

    db.InventoryItems.Add(new InventoryItem
    {
      PropertyId = propertyId,
      UnitTypeId = unitTypeId,
      Date = date,
      AvailableInventory = 8,
      LastSnapshotUtc = baselineTime,
      RecentEventIds = Enumerable.Range(1, 20).Select(i => $"evt-{i}").ToList()
    });
    await db.SaveChangesAsync();
    db.ChangeTracker.Clear();

    var replayedSnapshot = new InventorySnapshotUpdated(
        EventId: "evt-evicted-snapshot",
        Timestamp: baselineTime,
        PropertyId: propertyId,
        UnitTypeId: unitTypeId,
        Counts: [new NightlyInventory(date, 25)]
    );

    var seededAt = _timeProvider.UtcNow;
    _timeProvider.UtcNow = seededAt.AddHours(1);
    await EventProcessorWorker.HandleSnapshotAsync(db, replayedSnapshot);
    db.ChangeTracker.Clear();
    var result = await db.InventoryItems.SingleAsync(x => x.PropertyId == propertyId && x.Date == date);

    Assert.Equal(8, result.AvailableInventory);
    Assert.Equal(baselineTime, result.LastSnapshotUtc);
    Assert.DoesNotContain(replayedSnapshot.EventId, result.RecentEventIds);
    Assert.Equal(seededAt, result.CreatedAtUtc);
    Assert.Equal(seededAt, result.LastUpdatedUtc);
  }

  private sealed class TestTimeProvider(DateTimeOffset utcNow) : TimeProvider
  {
    public DateTimeOffset UtcNow { get; set; } = utcNow;
    public override DateTimeOffset GetUtcNow() => UtcNow;
  }
}
