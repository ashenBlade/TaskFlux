using System.Net.Sockets;
using Raft.Peer.Exceptions;

namespace Raft.Peer.Decorators;

public class NetworkExceptionWrapperDecorator: ISocket
{
    private readonly ISocket _socketImplementation;

    public NetworkExceptionWrapperDecorator(ISocket socketImplementation)
    {
        _socketImplementation = socketImplementation;
    }

    public void Dispose()
    {
        _socketImplementation.Dispose();
    }

    public async Task SendAsync(ReadOnlyMemory<byte> payload, CancellationToken token = default)
    {
        try
        {
            await _socketImplementation.SendAsync(payload, token);
        }
        catch (SocketException socket) when (NetworkException.IsNetworkError(socket.SocketErrorCode))
        {
            throw new NetworkException(socket);
        }
    }

    public async ValueTask ReadAsync(Stream stream, CancellationToken token = default)
    {
        try
        {
            await _socketImplementation.ReadAsync(stream, token);
        }
        catch (SocketException e) when (NetworkException.IsNetworkError(e.SocketErrorCode))
        {
            throw new NetworkException(e);
        }
    }
}