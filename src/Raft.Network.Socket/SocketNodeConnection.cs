using System.ComponentModel;
using System.Net.Sockets;
using Raft.Network.Packets;

namespace Raft.Network.Socket;

public class SocketNodeConnection: INodeConnection
{
    private const int DefaultBufferSize = 128;
    
    protected readonly System.Net.Sockets.Socket Socket;

    public SocketNodeConnection(System.Net.Sockets.Socket socket)
    {
        Socket = socket;
    }


    public async ValueTask<bool> SendAsync(IPacket packet, CancellationToken token = default)
    {
        var buffer = Serialize(packet);
        try
        {
            var sent = await Socket.SendAsync(buffer, SocketFlags.None, token);

            while (sent < buffer.Length)
            {
                var currentSent = await Socket.SendAsync(buffer.AsMemory(sent), SocketFlags.None, token);
                sent += currentSent;
            }

            return true;
        }
        catch (SocketException se)
            when (IsNetworkError(se.SocketErrorCode))
        {
            return false;
        }
    }

    public async ValueTask<IPacket?> ReceiveAsync(CancellationToken token = default)
    {
        using var memory = new MemoryStream();
        try
        {
            var buffer = new byte[DefaultBufferSize];
            var received = await Socket.ReceiveAsync(buffer, SocketFlags.None, token);
            if (received == 0)
            {
                return null;
            }

            memory.Write(buffer, 0, received);
        
            while (0 < Socket.Available)
            {
                received = await Socket.ReceiveAsync(buffer, SocketFlags.None, token);
                memory.Write(buffer, 0, received);
            }
        }
        catch (SocketException se) 
            when (IsNetworkError(se.SocketErrorCode))
        {
            return null;
        }

        var array = memory.ToArray();
        
        return array is {Length:>0}
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
    
    private static bool IsNetworkError(SocketError se)
    {
        return se is 
                   SocketError.Shutdown or 
                   SocketError.NotConnected or 
                   SocketError.Disconnecting or 
                   SocketError.ConnectionReset or 
                   SocketError.ConnectionAborted or 
                   SocketError.ConnectionRefused or 
                   SocketError.NetworkDown or 
                   SocketError.NetworkReset or 
                   SocketError.NetworkUnreachable;
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
}