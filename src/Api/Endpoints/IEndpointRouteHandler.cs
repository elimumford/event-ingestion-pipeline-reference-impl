namespace Api.Endpoints;

public interface IEndpointRouteHandler
{
  static abstract void Map(IEndpointRouteBuilder app);
}
