namespace IntegrationTests.Endpoints;

using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Nodes;
using Api.Domain.Events;
using Api.Domain.Inventory;
using Api.Workers;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

public class EventEndpointsTests(TestApiApplication factory) : IClassFixture<TestApiApplication>
{
  private readonly HttpClient _client = factory.CreateClient();
  private readonly TestEventPublisher _publisher = factory.Publisher;

  [Fact]
  public void HttpFixture_DoesNotStartPipelineWorkers()
  {
    var hostedServices = factory.Services.GetServices<IHostedService>();
    Assert.DoesNotContain(hostedServices, service => service is SqsConsumerWorker or EventProcessorWorker);
  }

  [Fact]
  public async Task Liveness_DoesNotRequireExternalServices()
  {
    var response = await _client.GetAsync("/livez");

    Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    Assert.Equal("Healthy", await response.Content.ReadAsStringAsync());
  }

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
    var response = await _client.PostAsJsonAsync("/api/v1/inventory/snapshot", payload);

    // Assert
    Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    Assert.Null(response.Headers.Location);
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
    var response = await _client.PostAsJsonAsync("/api/v1/inventory/snapshot", invalidPayload);

    // Assert
    Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
  }

  [Fact]
  public async Task IngestDelta_ValidPayload_ReturnsAcceptedAndPublishes()
  {
    // Arrange
    var payload = new InventoryDeltaAdjusted(
        EventId: Guid.NewGuid().ToString(),
        PropertyId: "prop-slc-01",
        UnitTypeId: "unit-deluxe-queen",
        Adjustments: [new NightlyInventoryAdjustment(new DateOnly(2026, 11, 10), -2)],
        Timestamp: DateTimeOffset.UtcNow
    );

    // Act
    var response = await _client.PostAsJsonAsync("/api/v1/inventory/delta", payload);

    // Assert
    Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    Assert.Null(response.Headers.Location);
    Assert.Contains(_publisher.PublishedEvents, e => e.EventId == payload.EventId);
  }

  [Fact]
  public async Task IngestDelta_EmptyAdjustments_ReturnsBadRequest()
  {
    // Arrange
    var invalidPayload = new InventoryDeltaAdjusted(
        EventId: Guid.NewGuid().ToString(),
        PropertyId: "prop-slc-01",
        UnitTypeId: "unit-deluxe-queen",
        Adjustments: [],
        Timestamp: DateTimeOffset.UtcNow
    );

    // Act
    var response = await _client.PostAsJsonAsync("/api/v1/inventory/delta", invalidPayload);

    // Assert
    Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
  }

  [Theory]
  [InlineData("snapshot", "counts", "null")]
  [InlineData("snapshot", "counts", "[]")]
  [InlineData("delta", "adjustments", "null")]
  [InlineData("delta", "adjustments", "[]")]
  [InlineData("snapshot", "eventId", "null")]
  [InlineData("snapshot", "eventId", "\"\"")]
  [InlineData("delta", "eventId", "\" \"")]
  [InlineData("snapshot", "propertyId", "null")]
  [InlineData("delta", "unitTypeId", "\" \"")]
  [InlineData("snapshot", "counts", "[{\"date\":\"2026-11-20\",\"availableInventory\":-1}]")]
  public async Task Ingest_InvalidContract_ReturnsBadRequestWithoutPublishing(
      string route, string field, string jsonValue)
  {
    var payload = CreateValidPayload();
    payload[field] = JsonNode.Parse(jsonValue);
    var publishedBefore = _publisher.PublishedEvents.Count;

    var response = await _client.PostAsJsonAsync($"/api/v1/inventory/{route}", payload);

    Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    Assert.Equal(publishedBefore, _publisher.PublishedEvents.Count);
  }

  [Theory]
  [InlineData("snapshot")]
  [InlineData("delta")]
  public async Task Ingest_MissingEventId_ReturnsBadRequestWithoutPublishing(string route)
  {
    var payload = CreateValidPayload();
    payload.Remove("eventId");
    var publishedBefore = _publisher.PublishedEvents.Count;

    var response = await _client.PostAsJsonAsync($"/api/v1/inventory/{route}", payload);

    Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    Assert.Equal(publishedBefore, _publisher.PublishedEvents.Count);
  }

  [Theory]
  [InlineData("snapshot", "propertyId")]
  [InlineData("snapshot", "unitTypeId")]
  [InlineData("delta", "propertyId")]
  [InlineData("delta", "unitTypeId")]
  public async Task Ingest_IdentifierTooLong_ReturnsBadRequestWithoutPublishing(string route, string field)
  {
    var payload = CreateValidPayload();
    payload[field] = new string('x', 129);
    var publishedBefore = _publisher.PublishedEvents.Count;

    var response = await _client.PostAsJsonAsync($"/api/v1/inventory/{route}", payload);

    Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    Assert.Equal(publishedBefore, _publisher.PublishedEvents.Count);
  }

  [Theory]
  [InlineData("snapshot")]
  [InlineData("delta")]
  public async Task Ingest_BoundaryValues_ReturnsAcceptedAndPublishes(string route)
  {
    var payload = CreateValidPayload();
    payload["propertyId"] = new string('p', 128);
    payload["unitTypeId"] = new string('u', 128);
    var eventId = $"opaque-event-{Guid.NewGuid():N}";
    payload["eventId"] = eventId;

    var response = await _client.PostAsJsonAsync($"/api/v1/inventory/{route}", payload);

    Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    Assert.Contains(_publisher.PublishedEvents, item => item.EventId == eventId);
  }

  private static JsonObject CreateValidPayload() => new()
  {
    ["eventId"] = $"evt-{Guid.NewGuid():N}",
    ["propertyId"] = "property",
    ["unitTypeId"] = "unit",
    ["timestamp"] = "2026-11-20T12:00:00Z",
    ["counts"] = new JsonArray(new JsonObject
    {
      ["date"] = "2026-11-20",
      ["availableInventory"] = 0
    }),
    ["adjustments"] = new JsonArray(new JsonObject
    {
      ["date"] = "2026-11-20",
      ["availableInventoryAdjustment"] = -1
    })
  };
}
