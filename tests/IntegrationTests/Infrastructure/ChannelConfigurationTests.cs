namespace IntegrationTests.Infrastructure;

using System.Globalization;
using System.Threading.Channels;
using Api.Infrastructure.Queue;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

public class ChannelConfigurationTests(TestApiApplication factory) : IClassFixture<TestApiApplication>
{
  [Theory]
  [InlineData(null, 250)]
  [InlineData(2, 2)]
  public async Task Channel_WaitsAtConfiguredCapacity_AndResumesWhenSpaceIsAvailable(
      int? capacity, int expectedCapacity)
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
    await using var application = CreateApplication(capacity);
    var channel = application.Services.GetRequiredService<Channel<InventoryEventEnvelope>>();
    var envelope = new InventoryEventEnvelope("message", "receipt", "unused", "{}", default);
    for (var i = 0; i < expectedCapacity; i++)
    {
      Assert.True(channel.Writer.TryWrite(envelope));
    }

    var waiting = channel.Writer.WriteAsync(envelope, timeout.Token).AsTask();
    Assert.False(waiting.IsCompleted);
    Assert.True(channel.Reader.TryRead(out _));
    await waiting.WaitAsync(timeout.Token);
    Assert.Equal(expectedCapacity, channel.Reader.Count);
  }

  [Theory]
  [InlineData(0)]
  [InlineData(-1)]
  public async Task Channel_NonpositiveCapacity_FailsDuringStartup(int capacity)
  {
    await using var application = CreateApplication(capacity);

    var error = Assert.Throws<OptionsValidationException>(() => application.CreateClient());

    Assert.Contains("Channel capacity must be greater than zero.", error.Failures);
  }

  private WebApplicationFactory<Program> CreateApplication(int? capacity) =>
      factory.WithWebHostBuilder(builder => builder.ConfigureAppConfiguration((_, configuration) =>
      {
        if (capacity.HasValue)
        {
          configuration.AddInMemoryCollection(new Dictionary<string, string?>
          {
            ["Channel:Capacity"] = capacity.Value.ToString(CultureInfo.InvariantCulture)
          });
        }
      }));
}
