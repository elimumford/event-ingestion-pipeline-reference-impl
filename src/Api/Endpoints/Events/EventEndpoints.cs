namespace Api.Endpoints.Events;

using Api.Domain.Events;
using Api.Endpoints.Filters;
using Api.Infrastructure;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

public static class EventEndpoints
{
  public static RouteGroupBuilder MapEventEndpoints(this IEndpointRouteBuilder app)
  {
    var group = app.MapGroup("/api/v1/events")
                   .WithTags("Events");

    group.MapPost("/snapshot", HandleSnapshotAsync)
         .WithName("IngestSnapshot")
         .AddEndpointFilter<ValidationFilter<InventorySnapshotUpdated>>();

    group.MapPost("/delta", HandleDeltaAsync)
         .WithName("IngestDelta")
         .AddEndpointFilter<ValidationFilter<InventoryDeltaAdjusted>>();

    return group;
  }

  private static async Task<Results<Accepted, ValidationProblem>> HandleSnapshotAsync(
      InventorySnapshotUpdated payload,
      [FromKeyedServices("sqs-publisher")] IEventPublisher publisher,
      CancellationToken ct)
  {
    await publisher.PublishAsync(payload, ct);
    return TypedResults.Accepted($"/api/v1/events/status/{payload.EventId}");
  }

  private static async Task<Results<Accepted, ValidationProblem>> HandleDeltaAsync(
      InventoryDeltaAdjusted payload,
      [FromKeyedServices("sqs-publisher")] IEventPublisher publisher,
      CancellationToken ct)
  {
    await publisher.PublishAsync(payload, ct);
    return TypedResults.Accepted($"/api/v1/events/status/{payload.EventId}");
  }
}
