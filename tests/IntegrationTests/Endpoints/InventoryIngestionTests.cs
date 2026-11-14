using System.Net;
using System.Net.Http.Json;
using Api.Domain.Events;
using Api.Domain.Inventory;
using IntegrationTests.Infrastructure;
using Xunit;

namespace IntegrationTests.Endpoints;

public class InventoryIngestionTests : IClassFixture<CustomWebApplicationFactory>
{
  private readonly HttpClient _client;

  public InventoryIngestionTests(CustomWebApplicationFactory factory)
  {
    _client = factory.CreateClient();
  }

  [Fact]
  public async Task IngestionEndpoint_ValidSnapshotPayload_ReturnsAccepted()
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
  }
}
