using System.Net;

namespace TaskFlux.Host.Modules.HttpRequest;

public interface IRequestHandler
{
    public Task HandleRequestAsync(HttpListenerRequest request, HttpListenerResponse response, CancellationToken token);
}