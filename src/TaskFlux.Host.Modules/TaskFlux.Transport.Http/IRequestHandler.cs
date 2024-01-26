using System.Net;

namespace TaskFlux.Transport.Http;

public interface IRequestHandler
{
    public Task HandleRequestAsync(HttpListenerRequest request, HttpListenerResponse response, CancellationToken token);
}