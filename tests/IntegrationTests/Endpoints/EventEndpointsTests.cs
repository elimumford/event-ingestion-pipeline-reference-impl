namespace IntegrationTests.Endpoints;

using System.Net;
using System.Net.Http.Json;
using Api.Domain.Events;
using Api.Domain.Inventory;
using IntegrationTests.Infrastructure;
using Xunit;

public class EventEndpointsTests(TestApiApplication factory) : IClassFixture<TestApiApplication>
{
  private readonly HttpClient _client = factory.CreateClient();
  private readonly TestEventPublisher _publisher = factory.Publisher;

  [Fact]
  public async Task IngestSnapshot_ValidPayload_ReturnsAcceptedAndPublishes()
  {
    // Arrange
    var payload = new InventorySnapshotUpdated(
        EventId: Guid.NewGuid().ToString(),
        PropertyId: "prop-slc-01",
        UnitTypeId: "unit-deluxe-queen",
        Counts: [new NightlyInventory(new DateOnly(2026, 11, 10), 5)],
        Timestamp: DateTimeOffset.UtcNow
    );

    // Act
    var response = await _client.PostAsJsonAsync("/api/v1/events/snapshot", payload);

    // Assert
    Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    Assert.NotNull(response.Headers.Location);
    Assert.Contains(_publisher.PublishedEvents, e => e.EventId == payload.EventId);
  }

  [Fact]
  public async Task IngestSnapshot_EmptyCounts_ReturnsBadRequest()
  {
    // Arrange
    var invalidPayload = new InventorySnapshotUpdated(
        EventId: Guid.NewGuid().ToString(),
        PropertyId: "prop-slc-01",
        UnitTypeId: "unit-deluxe-queen",
        Counts: [],
        Timestamp: DateTimeOffset.UtcNow
    );

    // Act
    var response = await _client.PostAsJsonAsync("/api/v1/events/snapshot", invalidPayload);

    // Assert
    Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
  }
}
