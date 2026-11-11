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
    await EventProcessorWorker.HandleSnapshotAsync(dbContext, snapshot, CancellationToken.None);
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
    await EventProcessorWorker.HandleDeltaAsync(dbContext, delta, CancellationToken.None);
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
}
