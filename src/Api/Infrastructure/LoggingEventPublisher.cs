namespace Api.Infrastructure;

using System.Text.Json;
using Api.Domain.Events;
using Microsoft.Extensions.Logging;

public sealed class LoggingEventPublisher(ILogger<LoggingEventPublisher> logger) : IEventPublisher
{
  public ValueTask PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
      where TEvent : IInventoryEvent
  {
    if (logger.IsEnabled(LogLevel.Information))
    {
      logger.LogInformation(
          "Ingested event {EventType} [{EventId}] for Property {PropertyId}: {Payload}",
          typeof(TEvent).Name,
          @event.EventId,
          @event.PropertyId,
          JsonSerializer.Serialize(@event)
      );
    }

    return ValueTask.CompletedTask;
  }
}
