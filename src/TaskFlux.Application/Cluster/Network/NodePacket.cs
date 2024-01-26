using System.Buffers;
using System.Runtime.CompilerServices;
using TaskFlux.Consensus.Cluster.Network.Exceptions;
using TaskFlux.Consensus.Cluster.Network.Packets;

namespace TaskFlux.Consensus.Cluster.Network;

public abstract class NodePacket
{
    internal NodePacket()
    {
    }

    public abstract NodePacketType PacketType { get; }
    protected abstract int EstimatePacketSize();
    protected abstract void SerializeBuffer(Span<byte> buffer);

    public async ValueTask SerializeAsync(Stream stream, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var estimatedSize = EstimatePacketSize();
        var buffer = ArrayPool<byte>.Shared.Rent(estimatedSize);
        try
        {
            SerializeBuffer(buffer.AsSpan(0, estimatedSize));
            await stream.WriteAsync(buffer.AsMemory(0, estimatedSize), token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public void Serialize(Stream stream)
    {
        var estimatedSize = EstimatePacketSize();
        var buffer = ArrayPool<byte>.Shared.Rent(estimatedSize);
        try
        {
            var span = buffer.AsSpan(0, estimatedSize);
            SerializeBuffer(span);
            stream.Write(span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public static NodePacket Deserialize(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[1];
        stream.ReadExactly(buffer);
        var packetType = ( NodePacketType ) buffer[0];
        try
        {
            return packetType switch
                   {
                       NodePacketType.AppendEntriesRequest  => AppendEntriesRequestPacket.Deserialize(stream),
                       NodePacketType.AppendEntriesResponse => AppendEntriesResponsePacket.Deserialize(stream),
                       NodePacketType.RequestVoteResponse   => RequestVoteResponsePacket.Deserialize(stream),
                       NodePacketType.RequestVoteRequest    => RequestVoteRequestPacket.Deserialize(stream),
                       NodePacketType.ConnectRequest        => ConnectRequestPacket.Deserialize(stream),
                       NodePacketType.ConnectResponse       => ConnectResponsePacket.Deserialize(stream),
                       NodePacketType.InstallSnapshotChunkRequest => InstallSnapshotChunkRequestPacket.Deserialize(
                           stream),
                       NodePacketType.InstallSnapshotResponse      => InstallSnapshotResponsePacket.Deserialize(stream),
                       NodePacketType.InstallSnapshotRequest       => InstallSnapshotRequestPacket.Deserialize(stream),
                       NodePacketType.RetransmitRequest            => new RetransmitRequestPacket(),
                       NodePacketType.InstallSnapshotChunkResponse => new InstallSnapshotChunkResponsePacket(),
                   };
        }
        catch (SwitchExpressionException)
        {
            throw new UnknownPacketException(buffer[0]);
        }
    }

    public static async Task<NodePacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(NodePacketType));
        byte marker;
        try
        {
            await stream.ReadExactlyAsync(buffer.AsMemory(0, sizeof(NodePacketType)), token);
            marker = buffer[0];
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        var packetType = ( NodePacketType ) marker;
        try
        {
            return packetType switch
                   {
                       NodePacketType.AppendEntriesRequest => await AppendEntriesRequestPacket.DeserializeAsync(stream,
                                                                  token),
                       NodePacketType.AppendEntriesResponse => await AppendEntriesResponsePacket.DeserializeAsync(
                                                                   stream, token),
                       NodePacketType.RequestVoteResponse => await RequestVoteResponsePacket.DeserializeAsync(stream,
                                                                 token),
                       NodePacketType.RequestVoteRequest => await RequestVoteRequestPacket.DeserializeAsync(stream,
                                                                token),
                       NodePacketType.ConnectRequest  => await ConnectRequestPacket.DeserializeAsync(stream, token),
                       NodePacketType.ConnectResponse => await ConnectResponsePacket.DeserializeAsync(stream, token),
                       NodePacketType.InstallSnapshotChunkRequest => await InstallSnapshotChunkRequestPacket
                                                                        .DeserializeAsync(stream, token),
                       NodePacketType.InstallSnapshotResponse =>
                           await InstallSnapshotResponsePacket.DeserializeAsync(stream, token),
                       NodePacketType.InstallSnapshotRequest =>
                           await InstallSnapshotRequestPacket.DeserializeAsync(stream, token),
                       NodePacketType.RetransmitRequest            => new RetransmitRequestPacket(),
                       NodePacketType.InstallSnapshotChunkResponse => new InstallSnapshotChunkResponsePacket(),
                   };
        }
        catch (SwitchExpressionException)
        {
            throw new UnknownPacketException(marker);
        }
    }

    internal static RentedBuffer Rent(int size)
    {
        return RentedBuffer.Create(size);
    }
}