namespace IntegrationTests.Infrastructure;

using Amazon.SQS;
using Api.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Polly;
using Polly.Registry;
using Xunit;

public class ResiliencePipelineTests
{
  private readonly ResiliencePipeline _pipeline;

  public ResiliencePipelineTests()
  {
    var services = new ServiceCollection();
    services.AddCustomResiliencePipelines();
    var provider = services.BuildServiceProvider();
    var pipelineProvider = provider.GetRequiredService<ResiliencePipelineProvider<string>>();
    _pipeline = pipelineProvider.GetPipeline(ResiliencePipelineExtensions.SqsPipelineKey);
  }

  [Fact]
  public async Task Pipeline_TransientSqsException_RetriesAndSucceeds()
  {
    // Arrange: fails twice with AmazonSQSException, succeeds on the 3rd attempt
    int attempts = 0;

    // Act
    var result = await _pipeline.ExecuteAsync(async ct =>
    {
      attempts++;
      if (attempts < 3)
      {
        throw new AmazonSQSException("Transient queue error");
      }
      await Task.Yield();
      return "success";
    });

    // Assert: standard xUnit verification
    Assert.Equal("success", result);
    Assert.Equal(3, attempts);
  }

  [Fact]
  public async Task Pipeline_ExhaustedRetries_ThrowsOriginalException()
  {
    // Arrange & Act & Assert
    int attempts = 0;

    await Assert.ThrowsAsync<AmazonSQSException>(async () =>
    {
      await _pipeline.ExecuteAsync<string>(async ct =>
          {
            attempts++;
            await Task.Yield();
            throw new AmazonSQSException("Persistent outage");
          });
    });

    // MaxRetryAttempts = 3 means 1 initial call + 3 retries = 4 total attempts
    Assert.Equal(4, attempts);
  }
}
