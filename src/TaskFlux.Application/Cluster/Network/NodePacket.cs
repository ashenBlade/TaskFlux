using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using TaskFlux.Application.Cluster.Network.Exceptions;
using TaskFlux.Application.Cluster.Network.Packets;

namespace TaskFlux.Application.Cluster.Network;

public abstract class NodePacket
{
    internal NodePacket()
    {
    }

    public abstract NodePacketType PacketType { get; }
    protected abstract int EstimatePayloadSize();
    protected abstract void SerializeBuffer(Span<byte> buffer);

    public async ValueTask SerializeAsync(Stream stream, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var estimatedSize = EstimatePayloadSize();
        var buffer = ArrayPool<byte>.Shared.Rent(estimatedSize);
        try
        {
            SerializeBuffer(buffer.AsSpan(0, estimatedSize));
            var watch = Stopwatch.StartNew();
            try
            {
                await SendPayloadAsync(stream, buffer.AsMemory(0, estimatedSize), token);
                Metrics.RpcSentBytes.Add(estimatedSize);
            }
            finally
            {
                watch.Stop();
                Metrics.RpcDuration.Record(watch.ElapsedMilliseconds);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private async Task SendPayloadAsync(Stream stream, Memory<byte> payload, CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(byte));
        try
        {
            buffer[0] = ( byte ) PacketType;
            await stream.WriteAsync(buffer.AsMemory(0, 1), token);
            await stream.WriteAsync(payload, token);
            await stream.FlushAsync(token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
    // TODO: положение полей в пакетах пересмотреть

    private void SendPayload(Stream stream, Span<byte> payload)
    {
        stream.WriteByte(( byte ) PacketType);
        stream.Write(payload);
        stream.Flush();
    }

    public void Serialize(Stream stream)
    {
        var estimatedSize = EstimatePayloadSize();
        var buffer = ArrayPool<byte>.Shared.Rent(estimatedSize);
        try
        {
            var span = buffer.AsSpan(0, estimatedSize);
            SerializeBuffer(span);
            var watch = Stopwatch.StartNew();
            try
            {
                SendPayload(stream, span);
                Metrics.RpcSentBytes.Add(estimatedSize);
            }
            finally
            {
                watch.Stop();
                Metrics.RpcDuration.Record(watch.ElapsedMilliseconds);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public static NodePacket Deserialize(Stream stream)
    {
        var marker = stream.ReadByte();
        if (marker == -1)
        {
            throw new EndOfStreamException("Соединение было разорвано");
        }

        try
        {
            return ( NodePacketType ) marker switch
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
            throw new UnknownPacketException(( byte ) marker);
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