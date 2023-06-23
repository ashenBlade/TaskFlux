using System.Net;

namespace Raft.Server.HttpModule;

public interface IRequestHandler
{
    public Task HandleRequestAsync(HttpListenerRequest request, HttpListenerResponse response, CancellationToken token);
}