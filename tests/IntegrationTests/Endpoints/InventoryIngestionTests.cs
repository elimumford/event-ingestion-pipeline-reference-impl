using System.Net;
using System.Net.Http.Json;
using Api.Domain.Entities;
using Api.Domain.Events;
using Api.Domain.Inventory;
using Api.Infrastructure.Persistence;
using Api.Workers;
using IntegrationTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace IntegrationTests.Endpoints;

public class InventoryIngestionTests : IClassFixture<CustomWebApplicationFactory>
{
  private readonly HttpClient _client;
  private readonly CustomWebApplicationFactory _factory;

  public InventoryIngestionTests(CustomWebApplicationFactory factory)
  {
    _factory = factory;
    _client = factory.CreateClient();
  }

  [Fact]
  public async Task IngestionEndpoint_ValidSnapshotPayload_PublishesWithoutInlinePersistence()
  {
    // Arrange
    var date = DateOnly.FromDateTime(DateTime.UtcNow);
    var payload = new InventorySnapshotUpdated(
        EventId: Guid.NewGuid().ToString(),
        Timestamp: DateTimeOffset.UtcNow,
        PropertyId: "prop-tc-1",
        UnitTypeId: "standard",
        Counts: [new NightlyInventory(date, 45)]
    );

    // Act
    var response = await _client.PostAsJsonAsync("/api/v1/inventory/snapshot", payload);

    // Assert
    Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    var published = Assert.Single(_factory.Publisher.PublishedEvents, item => item.EventId == payload.EventId);
    var snapshot = Assert.IsType<InventorySnapshotUpdated>(published);
    Assert.Equal(payload.PropertyId, snapshot.PropertyId);
    Assert.Equal(payload.UnitTypeId, snapshot.UnitTypeId);
    Assert.Equal(payload.Timestamp, snapshot.Timestamp);
    Assert.Equal(payload.Counts, snapshot.Counts);
    Assert.DoesNotContain(_factory.Services.GetServices<IHostedService>(),
        service => service is SqsConsumerWorker or EventProcessorWorker);

    await using var scope = _factory.Services.CreateAsyncScope();
    var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
    Assert.False(await db.InventoryItems.AnyAsync(item => item.PropertyId == payload.PropertyId));
  }

  [Fact]
  public async Task InventoryDbContext_ConcurrentUpdates_RejectStaleWrite()
  {
    var options = new DbContextOptionsBuilder<InventoryDbContext>()
        .UseNpgsql(_factory.ConnectionString)
        .Options;

    await using (var seedDb = new InventoryDbContext(options, TimeProvider.System))
    {
      seedDb.InventoryItems.Add(new InventoryItem
      {
        PropertyId = "prop-concurrency",
        UnitTypeId = "standard",
        Date = new DateOnly(2026, 11, 25),
        AvailableInventory = 10
      });
      await seedDb.SaveChangesAsync();
    }

    await using var firstDb = new InventoryDbContext(options, TimeProvider.System);
    await using var secondDb = new InventoryDbContext(options, TimeProvider.System);
    var firstWriter = await firstDb.InventoryItems.SingleAsync(x => x.PropertyId == "prop-concurrency");
    var secondWriter = await secondDb.InventoryItems.SingleAsync(x => x.PropertyId == "prop-concurrency");

    firstWriter.AvailableInventory = 8;
    await firstDb.SaveChangesAsync();

    secondWriter.AvailableInventory = 6;
    var conflict = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => secondDb.SaveChangesAsync());
    var classifier = _factory.Services.GetRequiredService<IInventoryConflictClassifier>();
    Assert.True(classifier.IsRetryableConflict(conflict));

    await using var verifyDb = new InventoryDbContext(options, TimeProvider.System);
    var persisted = await verifyDb.InventoryItems.SingleAsync(x => x.PropertyId == "prop-concurrency");
    Assert.Equal(8, persisted.AvailableInventory);
  }

  [Fact]
  public async Task InventoryConflictClassifier_RetriesPrimaryKeyViolationOnly()
  {
    var options = new DbContextOptionsBuilder<InventoryDbContext>()
        .UseNpgsql(_factory.ConnectionString)
        .Options;
    var propertyId = $"prop-{Guid.NewGuid():N}";
    var date = new DateOnly(2026, 11, 26);

    await using (var seedDb = new InventoryDbContext(options, TimeProvider.System))
    {
      seedDb.InventoryItems.Add(new InventoryItem
      {
        PropertyId = propertyId,
        UnitTypeId = "standard",
        Date = date
      });
      await seedDb.SaveChangesAsync();
    }

    await using var conflictingDb = new InventoryDbContext(options, TimeProvider.System);
    conflictingDb.InventoryItems.Add(new InventoryItem
    {
      PropertyId = propertyId,
      UnitTypeId = "standard",
      Date = date
    });

    var conflict = await Assert.ThrowsAsync<DbUpdateException>(() => conflictingDb.SaveChangesAsync());
    var classifier = _factory.Services.GetRequiredService<IInventoryConflictClassifier>();
    Assert.True(classifier.IsRetryableConflict(conflict));
    Assert.False(classifier.IsRetryableConflict(new DbUpdateException("Unrelated database error")));
  }
}
