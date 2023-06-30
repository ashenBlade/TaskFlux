using System.ComponentModel;
using System.Net;
using System.Net.Sockets;
using Raft.Network;
using Raft.Network.Packets;

namespace Raft.Peer;

public class PacketClient
{
    private const int DefaultBufferSize = 128;
    public Socket Socket { get; }

    public PacketClient(Socket socket)
    {
        Socket = socket;
    }

    private NetworkStream CreateStream() => new(Socket, false);

    public bool Send(IPacket packet, CancellationToken token = default)
    {
#pragma warning disable CA2012
        return SendCore(false, packet, token).GetAwaiter().GetResult();
#pragma warning restore CA2012
    }
    public ValueTask<bool> SendAsync(IPacket packet, CancellationToken token = default)
    {
        return SendCore(true, packet, token);
    }

    private async ValueTask<bool> SendCore(bool useAsync, IPacket packet, CancellationToken token)
    {
        if (!Socket.Connected)
        {
            return false;
        }
        
        var buffer = Serialize(packet);
        try
        {
            if (useAsync)
            {
                await using var stream = CreateStream();
                await stream.WriteAsync(buffer.AsMemory(), token);
            }
            else
            {
                // ReSharper disable once UseAwaitUsing
                using var stream = CreateStream();
                stream.Write(buffer.AsSpan());
            }

            return true;
        }
        catch (SocketException se)
            when (IsNetworkError(se.SocketErrorCode))
        {
            return false;
        }
        catch (IOException)
        {
            return false;
        }
    }
    
    public ValueTask<IPacket?> ReceiveAsync(CancellationToken token = default)
    {
        return ReceiveCoreAsync(true, token);
    }

    public IPacket? Receive(CancellationToken token = default)
    {
#pragma warning disable CA2012
        return ReceiveCoreAsync(false, token).GetAwaiter().GetResult();
#pragma warning restore CA2012
    }

    private async ValueTask<IPacket?> ReceiveCoreAsync(bool useAsync, CancellationToken token)
    {
        if (!Socket.Connected)
        {
            return null;
        }
        
        using var memory = new MemoryStream();
        // ReSharper disable once UseAwaitUsing
        using var stream = CreateStream();
        try
        {
            var buffer = new byte[DefaultBufferSize];
            var received = useAsync 
                               ? await stream.ReadAsync(buffer, token)
                               : stream.Read(buffer);
            if (received == 0 || !Socket.Connected)
            {
                if (useAsync)
                {
                    await Socket.DisconnectAsync(true, token);
                }
                else
                {
                    // ReSharper disable once MethodHasAsyncOverloadWithCancellation
                    Socket.Disconnect(true);
                }
                return null;
            }

            memory.Write(buffer, 0, received);

            while (0 < Socket.Available)
            {
                received = useAsync 
                               ? await stream.ReadAsync(buffer, token)
                               : stream.Read(buffer);
                if (received == 0)
                {
                    if (useAsync)
                    {
                        await Socket.DisconnectAsync(true, token);
                    }
                    else
                    {
                        // ReSharper disable once MethodHasAsyncOverloadWithCancellation
                        Socket.Disconnect(true);
                    }
                    return null;
                }

                memory.Write(buffer, 0, received);
            }
        }
        catch (SocketException se)
            when (IsNetworkError(se.SocketErrorCode))
        {
            return null;
        }
        catch (IOException)
        {
            return null;
        }

        var array = memory.ToArray();

        return array is {Length: > 0}
                   ? Deserialize(array)
                   : null;
    }

    private static byte[] Serialize(IPacket packet)
    {
        return packet.PacketType switch
               {
                   PacketType.ConnectRequest =>
                       Serializers.ConnectRequest.Serialize( packet.As<ConnectRequestPacket>() ),
                   PacketType.ConnectResponse => 
                       Serializers.ConnectResponse.Serialize(packet.As<ConnectResponsePacket>()),
                   
                   PacketType.AppendEntriesRequest => Serializers.AppendEntriesRequest.Serialize(
                       packet.As<AppendEntriesRequestPacket>().Request),
                   PacketType.AppendEntriesResponse => Serializers.AppendEntriesResponse.Serialize(
                       packet.As<AppendEntriesResponsePacket>().Response),
                   
                   PacketType.RequestVoteRequest => Serializers.RequestVoteRequest.Serialize(
                       packet.As<RequestVoteRequestPacket>().Request),
                   PacketType.RequestVoteResponse => Serializers.RequestVoteResponse.Serialize(
                       packet.As<RequestVoteResponsePacket>().Response),
                   
                   _ => throw new InvalidEnumArgumentException(nameof(packet.PacketType), (byte) packet.PacketType, typeof(PacketType))
               };
    }
    
    private static bool IsNetworkError(SocketError error)
    {
        return error is 
                   SocketError.Shutdown or 
                   SocketError.NotConnected or 
                   SocketError.Disconnecting or 
                   
                   SocketError.ConnectionReset or 
                   SocketError.ConnectionAborted or 
                   SocketError.ConnectionRefused or 
                   
                   SocketError.NetworkDown or 
                   SocketError.NetworkReset or 
                   SocketError.NetworkUnreachable or 
                   
                   SocketError.HostDown or 
                   SocketError.HostUnreachable or 
                   SocketError.HostNotFound;
    }

    private static IPacket Deserialize(byte[] buffer)
    {
        var marker = (PacketType) buffer[0];
        switch (marker)
        {
            case PacketType.ConnectResponse:
                return Serializers.ConnectResponse.Deserialize(buffer);
            case PacketType.ConnectRequest:
                return Serializers.ConnectRequest.Deserialize(buffer);
            case PacketType.RequestVoteRequest:
                return new RequestVoteRequestPacket(Serializers.RequestVoteRequest.Deserialize(buffer));
            case PacketType.RequestVoteResponse:
                return new RequestVoteResponsePacket(Serializers.RequestVoteResponse.Deserialize(buffer));
            case PacketType.AppendEntriesRequest:
                return new AppendEntriesRequestPacket(Serializers.AppendEntriesRequest.Deserialize(buffer));
            case PacketType.AppendEntriesResponse:
                return new AppendEntriesResponsePacket(Serializers.AppendEntriesResponse.Deserialize(buffer));
            default:
                throw new InvalidEnumArgumentException(nameof(marker), (int) marker, typeof(PacketType));
        }
    }

    public async ValueTask<bool> ConnectAsync(EndPoint endPoint, TimeSpan timeout, CancellationToken token = default)
    {
        if (Socket.Connected)
        {
            return true;
        }
        
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            cts.CancelAfter(timeout);
            await Socket.ConnectAsync(endPoint, cts.Token);
        }
        catch (SocketException)
        {
            return false;
        }
        // Незадокументировано
        catch (IOException io) when (io.GetBaseException() is SocketException)
        {
            return false;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        
        return true;
    }

    public async ValueTask DisconnectAsync(CancellationToken token = default)
    {
        if (Socket.Connected)
        {
            await Socket.DisconnectAsync(true, token);
        }
    }
}