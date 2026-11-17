
using Api.Domain.Events;

namespace Api.Endpoints.Filters;

public class ValidationFilter<T> : IEndpointFilter where T : class
{
  public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
  {
    var argument = context.Arguments.OfType<T>().FirstOrDefault();

    if (argument is null)
    {
      return TypedResults.BadRequest("Request payload cannot be null.");
    }

    if (argument is IInventoryEvent inventoryEvent)
    {
      if (string.IsNullOrWhiteSpace(inventoryEvent.EventId))
      {
        return TypedResults.BadRequest("EventId is required.");
      }

      if (string.IsNullOrWhiteSpace(inventoryEvent.PropertyId) ||
          inventoryEvent.PropertyId.Length > 128 ||
          string.IsNullOrWhiteSpace(inventoryEvent.UnitTypeId) ||
          inventoryEvent.UnitTypeId.Length > 128)
      {
        return TypedResults.BadRequest("PropertyId and UnitTypeId are required and must not exceed 128 characters.");
      }
    }

    if (argument is InventorySnapshotUpdated snapshot)
    {
      if (snapshot.Counts is not { Count: > 0 })
      {
        return TypedResults.BadRequest("Counts must contain at least one date.");
      }

      if (snapshot.Counts.Any(count => count.AvailableInventory < 0))
      {
        return TypedResults.BadRequest("Snapshot inventory counts must not be negative.");
      }
    }
    else if (argument is InventoryDeltaAdjusted delta)
    {
      if (delta.Adjustments is not { Count: > 0 })
      {
        return TypedResults.BadRequest("Adjustments must contain at least one date.");
      }
    }

    return await next(context);
  }
}
