namespace Api.Infrastructure;

using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Api.Configuration;
using Api.Domain.Events;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Registry;

public sealed class SqsEventPublisher(
    IAmazonSQS sqsClient,
    IOptions<SqsOptions> options,
    ResiliencePipelineProvider<string> pipelineProvider,
    ILogger<SqsEventPublisher> logger) : IEventPublisher
{
  private readonly SqsOptions _options = options.Value;
  private readonly ResiliencePipeline _resiliencePipeline =
      pipelineProvider.GetPipeline(ResiliencePipelineExtensions.SqsPipelineKey);

  public async ValueTask PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
      where TEvent : IInventoryEvent
  {
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

    // Execute send with automatic exponential retry + jitter
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
