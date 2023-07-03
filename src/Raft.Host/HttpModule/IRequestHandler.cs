using System.Net;

namespace Raft.Host.HttpModule;

public interface IRequestHandler
{
    public Task HandleRequestAsync(HttpListenerRequest request, HttpListenerResponse response, CancellationToken token);
}