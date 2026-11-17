namespace IntegrationTests.Endpoints;

using System.Net;
using System.Net.Http.Json;
using Amazon.SQS.Model;
using Api.Domain.Events;
using Api.Domain.Inventory;
using Api.Infrastructure.Persistence;
using DotNet.Testcontainers.Builders;
using IntegrationTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Testcontainers.PostgreSql;

public sealed class ContainerizedIngestionTests
{
  [Fact]
  public async Task Snapshot_FromContainerizedApi_IsPersistedAndAcknowledged()
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
    var ct = timeout.Token;
    var root = FindRepositoryRoot();
    await using var image = new ImageFromDockerfileBuilder()
        .WithDockerfileDirectory(root)
        .WithDockerfile("src/Api/Dockerfile")
        .Build();
    await image.CreateAsync(ct);

    await using var network = new NetworkBuilder().Build();
    await network.CreateAsync(ct);
    await using var postgres = new PostgreSqlBuilder(DevelopmentInfrastructure.PostgresImage)
        .WithDatabase("inventory_test_db")
        .WithUsername("postgres")
        .WithPassword("postgres")
        .WithNetwork(network)
        .WithNetworkAliases("postgres")
        .Build();
    await postgres.StartAsync(ct);
    var dbOptions = new DbContextOptionsBuilder<InventoryDbContext>()
        .UseNpgsql(postgres.GetConnectionString())
        .Options;
    await using (var migration = new InventoryDbContext(dbOptions, TimeProvider.System))
    {
      await migration.Database.MigrateAsync(ct);
    }

    await using var broker = DevelopmentInfrastructure.CreateBroker(network);
    await broker.StartAsync(ct);
    using var sqs = DevelopmentInfrastructure.CreateSqsClient(broker);
    var endpoint = new Uri(sqs.Config.ServiceURL);
    var queue = await sqs.GetQueueUrlAsync("inventory-events", ct);
    var queueUrl = new UriBuilder(queue.QueueUrl) { Host = endpoint.Host, Port = endpoint.Port }.Uri.ToString();

    var containerConnection = new NpgsqlConnectionStringBuilder(postgres.GetConnectionString())
    {
      Host = "postgres",
      Port = 5432
    }.ConnectionString;
    var brokerUrl = $"http://broker:{DevelopmentInfrastructure.BrokerPort}";
    await using var api = new ContainerBuilder(image)
        .WithNetwork(network)
        .WithPortBinding(8080, true)
        .WithEnvironment("ConnectionStrings__DefaultConnection", containerConnection)
        .WithEnvironment("Sqs__ServiceUrl", brokerUrl)
        .WithEnvironment("Sqs__QueueUrl", $"{brokerUrl}/000000000000/inventory-events")
        .WithEnvironment("Sqs__Region", "us-west-2")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(8080))
        .Build();
    await api.StartAsync(ct);
    using var client = new HttpClient
    {
      BaseAddress = new UriBuilder("http", api.Hostname, api.GetMappedPublicPort(8080)).Uri
    };
    using var readiness = await client.GetAsync("/healthz", ct);
    Assert.Equal(HttpStatusCode.OK, readiness.StatusCode);

    var date = new DateOnly(2026, 12, 1);
    var payload = new InventorySnapshotUpdated(
        Guid.NewGuid().ToString("N"), $"container-{Guid.NewGuid():N}", "standard",
        [new NightlyInventory(date, 42)], DateTimeOffset.UtcNow);
    using var response = await client.PostAsJsonAsync("/api/v1/inventory/snapshot", payload, ct);
    Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

    using var processingTimeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
    processingTimeout.CancelAfter(TimeSpan.FromSeconds(30));
    while (true)
    {
      await using var verify = new InventoryDbContext(dbOptions, TimeProvider.System);
      var item = await verify.InventoryItems.AsNoTracking().SingleOrDefaultAsync(
          item => item.PropertyId == payload.PropertyId && item.UnitTypeId == payload.UnitTypeId && item.Date == date,
          processingTimeout.Token);
      if (item is not null)
      {
        Assert.Equal(42, item.AvailableInventory);
        Assert.Contains(payload.EventId, item.RecentEventIds);
        var attributes = await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
          QueueUrl = queueUrl,
          AttributeNames = ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
        }, processingTimeout.Token);
        if (attributes.ApproximateNumberOfMessages == 0 && attributes.ApproximateNumberOfMessagesNotVisible == 0)
        {
          break;
        }
      }

      await Task.Delay(TimeSpan.FromMilliseconds(250), processingTimeout.Token);
    }
  }

  private static string FindRepositoryRoot()
  {
    for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
    {
      if (File.Exists(Path.Combine(directory.FullName, "event-ingestion-pipeline-reference-impl.slnx")))
      {
        return directory.FullName;
      }
    }

    throw new DirectoryNotFoundException("Cannot locate the repository root required to build the API image.");
  }
}
