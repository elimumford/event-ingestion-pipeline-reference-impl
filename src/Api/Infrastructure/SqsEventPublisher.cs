namespace Api.Infrastructure;

using System.Diagnostics;
using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Api.Configuration;
using Api.Domain.Events;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using Polly;
using Polly.Registry;

public sealed class SqsEventPublisher(
    IAmazonSQS sqsClient,
    IOptions<SqsOptions> options,
    ResiliencePipelineProvider<string> pipelineProvider,
    ILogger<SqsEventPublisher> logger) : IEventPublisher
{
  private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;
  private readonly SqsOptions _options = options.Value;
  private readonly ResiliencePipeline _resiliencePipeline =
      pipelineProvider.GetPipeline(ResiliencePipelineExtensions.SqsPipelineKey);

  public async ValueTask PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
      where TEvent : IInventoryEvent
  {
    using var activity = Diagnostics.ActivitySource.StartActivity("sqs.publish");

    var messageBody = JsonSerializer.Serialize(@event);

    var request = new SendMessageRequest
    {
      QueueUrl = _options.QueueUrl,
      MessageBody = messageBody,
      MessageAttributes = new Dictionary<string, MessageAttributeValue>
      {
        ["EventType"] = new()
        {
          DataType = "String",
          StringValue = typeof(TEvent).Name
        },
        ["EventId"] = new()
        {
          DataType = "String",
          StringValue = @event.EventId
        }
      }
    };

    // Inject active W3C trace context (both traceparent and tracestate)
    if (Activity.Current is { } currentActivity)
    {
      var propagationContext = new PropagationContext(currentActivity.Context, Baggage.Current);

      Propagator.Inject(
          propagationContext,
          request.MessageAttributes,
          static (attributes, key, value) =>
          {
            attributes[key] = new MessageAttributeValue
            {
              DataType = "String",
              StringValue = value
            };
          }
      );
    }

    var response = await _resiliencePipeline.ExecuteAsync(
        async ct => await sqsClient.SendMessageAsync(request, ct),
        cancellationToken
    );

    if (logger.IsEnabled(LogLevel.Information))
    {
      logger.LogInformation(
          "Dispatched {EventType} [{EventId}] to SQS. MessageId: {MessageId}",
          typeof(TEvent).Name,
          @event.EventId,
          response.MessageId
      );
    }
  }
}
