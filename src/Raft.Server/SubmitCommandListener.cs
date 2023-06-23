using System.Net;
using System.Net.Sockets;
using System.Text;
using Raft.Core.Commands.Submit;
using Raft.Core.Node;

namespace Raft.Server;

public class SubmitCommandListener
{
    private readonly INode _node;
    private readonly int _port;

    public SubmitCommandListener(INode node, int port)
    {
        _node = node;
        _port = port;
    }
    
    public async Task RunAsync(CancellationToken token)
    {
        await Task.Yield();
        var tcs = new TaskCompletionSource();
        await using var _ = token.Register(() => tcs.SetCanceled(token));
        var listener = new TcpListener(IPAddress.Any, _port);
        listener.Start();
        var encoding = Encoding.UTF8;
        try
        {
            while (token.IsCancellationRequested is false)
            {
                using var client = await listener.AcceptTcpClientAsync(token);
                await using var stream = client.GetStream();
                try
                {
                    var response = _node.Handle(new SubmitRequest(Random.Shared.Next().ToString()));
                    var writer = new StreamWriter(stream, encoding);
                    await writer.WriteAsync($"HTTP/1.1 200 OK\n");
                    var responseBody = $"{response.CreatedEntry}";
                    await writer.WriteAsync($"Content-Length: {encoding.GetByteCount(responseBody)}\n\n");
                    await writer.WriteAsync(responseBody);
                }
                catch (InvalidOperationException e)
                {
                    var writer = new StreamWriter(stream, encoding);
                    await writer.WriteAsync($"HTTP/1.1 400 OK\n");
                    var responseBody = $"Error: {e}";
                    await writer.WriteAsync($"Content-Length: {encoding.GetByteCount(responseBody)}\n\n");
                    await writer.WriteAsync(responseBody);
                }
                finally
                {
                    client.Close();
                }
            }
        }
        finally
        {
            listener.Stop();
        }
    }
}