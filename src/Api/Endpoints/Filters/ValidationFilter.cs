
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

    // Domain-level validation guards for inbound events
    if (argument is InventorySnapshotUpdated snapshot)
    {
      if (string.IsNullOrWhiteSpace(snapshot.PropertyId) ||
          string.IsNullOrWhiteSpace(snapshot.UnitTypeId) ||
          snapshot.Counts.Count == 0)
      {
        return TypedResults.BadRequest("PropertyId and UnitTypeId are required, and Counts must contain at least one date.");
      }
    }
    else if (argument is InventoryDeltaAdjusted delta)
    {
      if (string.IsNullOrWhiteSpace(delta.PropertyId) ||
          string.IsNullOrWhiteSpace(delta.UnitTypeId) ||
          delta.Adjustments.Count == 0)
      {
        return TypedResults.BadRequest("PropertyId and UnitTypeId are required, and Adjustments must contain at least one date.");
      }
    }

    return await next(context);
  }
}
