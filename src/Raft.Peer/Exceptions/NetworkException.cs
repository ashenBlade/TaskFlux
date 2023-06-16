using System.Net.Sockets;

namespace Raft.Peer.Exceptions;

public class NetworkException: Exception
{
    public SocketException SocketException { get; }

    public NetworkException(SocketException exception) : base(exception.Message, exception)
    {
        SocketException = exception;
    }

    internal static bool IsNetworkError(SocketError error)
    {
        return error is
                   SocketError.HostDown or
                   SocketError.HostUnreachable or
                   SocketError.HostNotFound or
                   SocketError.NetworkDown or
                   SocketError.NetworkUnreachable or
                   SocketError.NetworkReset or 
                   SocketError.ConnectionRefused or 
                   SocketError.ConnectionReset;
    }
}