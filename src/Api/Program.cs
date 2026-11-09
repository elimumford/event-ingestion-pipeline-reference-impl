using Api.Configuration;
using Api.Endpoints.Events;


var builder = WebApplication.CreateBuilder(args);

// 1. DI Composition
builder.Services.AddEventInfrastructure(builder.Configuration);

var app = builder.Build();

// 2. Middleware & Endpoints
app.MapHealthChecks("/healthz");
app.MapEventEndpoints();

app.Run();

// Required for WebApplicationFactory in integration tests
public partial class Program;
