namespace IntegrationTests;

using Api.Domain.Entities;
using Api.Domain.Events;
using Api.Domain.Inventory;
using Api.Infrastructure.Persistence;
using Api.Workers;
using Microsoft.EntityFrameworkCore;
using Xunit;

public class EventProcessorWorkerTests
{
  private static InventoryDbContext CreateInMemoryDbContext()
  {
    var options = new DbContextOptionsBuilder<InventoryDbContext>()
        .UseInMemoryDatabase(databaseName: Guid.NewGuid().ToString())
        .Options;

    return new InventoryDbContext(options);
  }

  [Fact]
  public async Task HandleSnapshotAsync_InsertsNewItems_AndUpdatesExistingItems()
  {
    // Arrange
    await using var dbContext = CreateInMemoryDbContext();
    var existingDate = new DateOnly(2026, 11, 10);
    var newDate = new DateOnly(2026, 11, 11);

    dbContext.InventoryItems.Add(new InventoryItem
    {
      PropertyId = "prop-slc-01",
      UnitTypeId = "unit-deluxe-queen",
      Date = existingDate,
      AvailableInventory = 3
    });
    await dbContext.SaveChangesAsync();

    var snapshot = new InventorySnapshotUpdated(
        EventId: Guid.NewGuid().ToString(),
        PropertyId: "prop-slc-01",
        UnitTypeId: "unit-deluxe-queen",
        Counts:
        [
            new NightlyInventory(existingDate, 10),
            new NightlyInventory(newDate, 7)
        ],
        Timestamp: DateTimeOffset.UtcNow
    );

    // Act
    await EventProcessorWorker.HandleSnapshotAsync(dbContext, snapshot, null, CancellationToken.None);
    await dbContext.SaveChangesAsync();

    // Assert
    var items = await dbContext.InventoryItems
        .OrderBy(i => i.Date)
        .ToListAsync();

    Assert.Equal(2, items.Count);
    Assert.Equal(existingDate, items[0].Date);
    Assert.Equal(10, items[0].AvailableInventory);
    Assert.Equal(newDate, items[1].Date);
    Assert.Equal(7, items[1].AvailableInventory);
  }

  [Fact]
  public async Task HandleDeltaAsync_AppliesAdjustments_AndClampsNegativeInventoryToZero()
  {
    // Arrange
    await using var dbContext = CreateInMemoryDbContext();
    var datePositiveDelta = new DateOnly(2026, 11, 10);
    var dateClampedExisting = new DateOnly(2026, 11, 11);
    var dateClampedNew = new DateOnly(2026, 11, 12);
    var datePositiveNew = new DateOnly(2026, 11, 13);

    dbContext.InventoryItems.AddRange(
        new InventoryItem
        {
          PropertyId = "prop-slc-01",
          UnitTypeId = "unit-deluxe-queen",
          Date = datePositiveDelta,
          AvailableInventory = 5
        },
        new InventoryItem
        {
          PropertyId = "prop-slc-01",
          UnitTypeId = "unit-deluxe-queen",
          Date = dateClampedExisting,
          AvailableInventory = 2
        }
    );
    await dbContext.SaveChangesAsync();

    var delta = new InventoryDeltaAdjusted(
        EventId: Guid.NewGuid().ToString(),
        PropertyId: "prop-slc-01",
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
    await dbContext.SaveChangesAsync();

    // Assert
    var items = await dbContext.InventoryItems
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
    using var db = CreateInMemoryDbContext();
    var propertyId = "prop-1";
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

    var deltaPayload = new InventoryDeltaAdjusted(
        EventId: "evt-delta-dup",
        Timestamp: DateTimeOffset.UtcNow,
        PropertyId: propertyId,
        UnitTypeId: unitTypeId,
        Adjustments: [new NightlyInventoryAdjustment(date, -2)]
    );

    // Act: First run applies adjustment
    await EventProcessorWorker.HandleDeltaAsync(db, deltaPayload);
    var firstState = await db.InventoryItems.FirstAsync(x => x.Date == date);
    Assert.Equal(8, firstState.AvailableInventory);
    Assert.Contains("evt-delta-dup", firstState.RecentEventIds);

    // Act: Second run with same EventId (simulating SQS replay)
    await EventProcessorWorker.HandleDeltaAsync(db, deltaPayload);
    var replayState = await db.InventoryItems.FirstAsync(x => x.Date == date);

    // Assert: Inventory remains 8 (not decremented to 6)
    Assert.Equal(8, replayState.AvailableInventory);
    Assert.Single(replayState.RecentEventIds, id => id == "evt-delta-dup");
  }

  [Fact]
  public async Task HandleDeltaAsync_DeltaPredatingLastSnapshot_IsDiscarded()
  {
    // Arrange
    using var db = CreateInMemoryDbContext();
    var propertyId = "prop-1";
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

    // Delta created 10 minutes BEFORE the snapshot
    var staleDelta = new InventoryDeltaAdjusted(
        EventId: "evt-stale-delta",
        Timestamp: snapshotTime.AddMinutes(-10),
        PropertyId: propertyId,
        UnitTypeId: unitTypeId,
        Adjustments: [new NightlyInventoryAdjustment(date, +3)]
    );

    // Act
    await EventProcessorWorker.HandleDeltaAsync(db, staleDelta);
    var result = await db.InventoryItems.FirstAsync(x => x.Date == date);

    // Assert: Delta is ignored because the snapshot baseline supersedes it
    Assert.Equal(5, result.AvailableInventory);
    Assert.DoesNotContain("evt-stale-delta", result.RecentEventIds);
  }

  [Fact]
  public async Task HandleDeltaAsync_RingBuffer_CapsAtMaxCapacity()
  {
    // Arrange
    using var db = CreateInMemoryDbContext();
    var propertyId = "prop-1";
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

    var newDelta = new InventoryDeltaAdjusted(
        EventId: "evt-newest-21",
        Timestamp: DateTimeOffset.UtcNow,
        PropertyId: propertyId,
        UnitTypeId: unitTypeId,
        Adjustments: [new NightlyInventoryAdjustment(date, +1)]
    );

    // Act
    await EventProcessorWorker.HandleDeltaAsync(db, newDelta);
    var result = await db.InventoryItems.FirstAsync(x => x.Date == date);

    // Assert: Buffer size remains 20, oldest entry evicted, newest appended
    Assert.Equal(20, result.RecentEventIds.Count);
    Assert.DoesNotContain("evt-initial-1", result.RecentEventIds);
    Assert.Equal("evt-newest-21", result.RecentEventIds.Last());
  }

  [Fact]
  public async Task HandleSnapshotAsync_StaleSnapshot_IsIgnored()
  {
    // Arrange
    using var db = CreateInMemoryDbContext();
    var propertyId = "prop-1";
    var unitTypeId = "standard";
    var date = new DateOnly(2026, 11, 23);
    var baselineTime = DateTimeOffset.UtcNow;

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

    // Snapshot with older timestamp
    var staleSnapshot = new InventorySnapshotUpdated(
        EventId: "evt-stale-snap",
        Timestamp: baselineTime.AddHours(-2),
        PropertyId: propertyId,
        UnitTypeId: unitTypeId,
        Counts: [new NightlyInventory(date, 25)]
    );

    // Act
    await EventProcessorWorker.HandleSnapshotAsync(db, staleSnapshot);
    var result = await db.InventoryItems.FirstAsync(x => x.Date == date);

    // Assert: Stale snapshot did not overwrite current baseline
    Assert.Equal(10, result.AvailableInventory);
    Assert.Equal(baselineTime, result.LastSnapshotUtc);
    Assert.DoesNotContain("evt-stale-snap", result.RecentEventIds);
  }
}
