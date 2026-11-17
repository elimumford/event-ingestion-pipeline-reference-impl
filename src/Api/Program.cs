using Api.Configuration;
using Api.Endpoints.Inventory;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;


var builder = WebApplication.CreateBuilder(args);

// 1. DI Composition
builder.Services.AddEventInfrastructure(builder.Configuration);

var app = builder.Build();

// 2. Middleware & Endpoints
app.MapHealthChecks("/healthz", new HealthCheckOptions
{
  Predicate = registration => registration.Tags.Contains("ready")
});
app.MapHealthChecks("/livez", new HealthCheckOptions
{
  Predicate = _ => false
});
app.MapEventEndpoints();

app.Run();

// Required for WebApplicationFactory in integration tests
public partial class Program;
