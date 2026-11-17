namespace IntegrationTests.Infrastructure;

using System.Text.Json;
using Amazon.SQS.Model;

public sealed class ElasticMqTopologyTests
{
  [Fact]
  public async Task DeclaredTopology_RedrivesAfterThreeUnacknowledgedReceives()
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
    var ct = timeout.Token;
    await using var broker = DevelopmentInfrastructure.CreateBroker();
    await broker.StartAsync(ct);
    using var sqs = DevelopmentInfrastructure.CreateSqsClient(broker);
    var endpoint = new Uri(sqs.Config.ServiceURL);
    string ResolveUrl(string url) => new UriBuilder(url)
    {
      Host = endpoint.Host,
      Port = endpoint.Port
    }.Uri.ToString();

    var queueUrl = ResolveUrl((await sqs.GetQueueUrlAsync("inventory-events", ct)).QueueUrl);
    var deadLetterUrl = ResolveUrl((await sqs.GetQueueUrlAsync("inventory-events-dlq", ct)).QueueUrl);
    var queueAttributes = await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
    {
      QueueUrl = queueUrl,
      AttributeNames = ["All"]
    }, ct);
    var deadLetterAttributes = await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
    {
      QueueUrl = deadLetterUrl,
      AttributeNames = ["All"]
    }, ct);

    Assert.Equal("60", queueAttributes.Attributes["VisibilityTimeout"]);
    Assert.Equal("60", deadLetterAttributes.Attributes["VisibilityTimeout"]);
    using var policy = JsonDocument.Parse(queueAttributes.Attributes["RedrivePolicy"]);
    Assert.Equal(deadLetterAttributes.QueueARN, policy.RootElement.GetProperty("deadLetterTargetArn").GetString());
    Assert.Equal(3, policy.RootElement.GetProperty("maxReceiveCount").GetInt32());

    var body = $"topology-probe-{Guid.NewGuid():N}";
    await sqs.SendMessageAsync(queueUrl, body, ct);
    for (var attempt = 1; attempt <= 3; attempt++)
    {
      var received = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
      {
        QueueUrl = queueUrl,
        WaitTimeSeconds = 1,
        MessageSystemAttributeNames = ["ApproximateReceiveCount"]
      }, ct);
      var message = Assert.Single(received.Messages);
      Assert.Equal(body, message.Body);
      Assert.Equal(attempt.ToString(), message.Attributes["ApproximateReceiveCount"]);
      await sqs.ChangeMessageVisibilityAsync(queueUrl, message.ReceiptHandle, 0, ct);
    }

    var exhausted = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
    {
      QueueUrl = queueUrl,
      WaitTimeSeconds = 1
    }, ct);
    Assert.Empty(exhausted.Messages);
    var deadLetters = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
    {
      QueueUrl = deadLetterUrl,
      WaitTimeSeconds = 1
    }, ct);
    Assert.Equal(body, Assert.Single(deadLetters.Messages).Body);
  }
}
