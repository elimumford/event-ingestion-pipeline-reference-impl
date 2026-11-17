namespace IntegrationTests.Infrastructure;

using System.Text.Json;
using Amazon.SQS;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

internal static class DevelopmentInfrastructure
{
  private static readonly JsonElement Definition = LoadDefinition();

  public static string PostgresImage => RequiredString(Service("postgres-dev"), "image");

  private static JsonElement Broker => Service("elasticmq");

  public static ushort BrokerPort => Broker.GetProperty("ports")[0].GetProperty("target").GetUInt16();

  public static IContainer CreateBroker(INetwork? network = null)
  {
    var config = Broker.GetProperty("volumes").EnumerateArray().Single(mount =>
        mount.GetProperty("type").GetString() == "bind");
    var source = RequiredString(config, "source");
    var configPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, source));
    if (!configPath.StartsWith(AppContext.BaseDirectory, StringComparison.Ordinal))
    {
      throw new InvalidDataException("The broker configuration must be inside the test output directory.");
    }

    var builder = new ContainerBuilder(RequiredString(Broker, "image"))
        .WithPortBinding(BrokerPort, true)
        .WithResourceMapping(FilePath.Of(configPath), FilePath.Of(RequiredString(config, "target")))
        .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(BrokerPort));

    return (network is null ? builder : builder.WithNetwork(network).WithNetworkAliases("broker")).Build();
  }

  public static AmazonSQSClient CreateSqsClient(IContainer broker) => new("test", "test", new AmazonSQSConfig
  {
    ServiceURL = new UriBuilder("http", broker.Hostname, broker.GetMappedPublicPort(BrokerPort)).Uri.ToString(),
    AuthenticationRegion = "us-east-1"
  });

  private static JsonElement Service(string name) => Definition.GetProperty("services").GetProperty(name);

  private static string RequiredString(JsonElement element, string property) =>
      element.GetProperty(property).GetString()
      ?? throw new InvalidDataException($"Missing development infrastructure value '{property}'.");

  private static JsonElement LoadDefinition()
  {
    using var document = JsonDocument.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "compose.json")));
    return document.RootElement.Clone();
  }
}
