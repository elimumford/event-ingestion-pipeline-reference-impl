namespace Api.Infrastructure.Queue;

using OpenTelemetry.Context.Propagation;

public sealed record InventoryEventEnvelope(
    string MessageId,
    string ReceiptHandle,
    string EventType,
    string Body,
    PropagationContext PropagationContext
);
