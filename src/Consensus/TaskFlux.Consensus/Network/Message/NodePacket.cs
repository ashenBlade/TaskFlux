using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using TaskFlux.Consensus.Network.Message.Exceptions;
using TaskFlux.Consensus.Network.Message.Packets;
using TaskFlux.Utils.CheckSum;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Network.Message;

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
        // Сразу выделяем место под нагрузку и чек-сумму
        var buffer = ArrayPool<byte>.Shared.Rent(estimatedSize + sizeof(uint));
        try
        {
            var checkSum = buffer.AsMemory(estimatedSize, sizeof(uint));
            var payload = buffer.AsMemory(0, estimatedSize);
            SerializeBuffer(payload.Span);
            ComputeChecksum(payload.Span, checkSum.Span);

            var watch = Stopwatch.StartNew();
            try
            {
                await SendPayloadAsync(stream, payload, checkSum, token);
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

    private async Task SendPayloadAsync(Stream stream,
        Memory<byte> payload,
        Memory<byte> checkSum,
        CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(byte));
        try
        {
            buffer[0] = (byte)PacketType;
            await stream.WriteAsync(buffer.AsMemory(0, 1), token);
            await stream.WriteAsync(payload, token);
            await stream.WriteAsync(checkSum, token);
            await stream.FlushAsync(token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void SendPayload(Stream stream, Span<byte> payload, Span<byte> checkSum)
    {
        stream.WriteByte((byte)PacketType);
        stream.Write(payload);
        stream.Write(checkSum);
        stream.Flush();
    }

    public void Serialize(Stream stream)
    {
        var estimatedSize = EstimatePayloadSize();
        // Сразу выделим место для нагрузки и чек-суммы
        var buffer = ArrayPool<byte>.Shared.Rent(estimatedSize + sizeof(int));
        try
        {
            var payload = buffer.AsSpan(0, estimatedSize);
            SerializeBuffer(payload);
            var checkSum = buffer.AsSpan(estimatedSize, sizeof(int));
            ComputeChecksum(payload, checkSum);
            var watch = Stopwatch.StartNew();
            try
            {
                SendPayload(stream, payload, checkSum);
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

    private void ComputeChecksum(Span<byte> payload, Span<byte> checkSum)
    {
        var computed = Crc32CheckSum.Compute(payload);
        var writer = new SpanBinaryWriter(checkSum);
        writer.Write(computed);
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
            return (NodePacketType)marker switch
            {
                NodePacketType.AppendEntriesRequest => AppendEntriesRequestPacket.Deserialize(stream),
                NodePacketType.AppendEntriesResponse => AppendEntriesResponsePacket.Deserialize(stream),
                NodePacketType.RequestVoteResponse => RequestVoteResponsePacket.Deserialize(stream),
                NodePacketType.RequestVoteRequest => RequestVoteRequestPacket.Deserialize(stream),
                NodePacketType.ConnectRequest => ConnectRequestPacket.Deserialize(stream),
                NodePacketType.ConnectResponse => ConnectResponsePacket.Deserialize(stream),
                NodePacketType.InstallSnapshotChunkRequest => InstallSnapshotChunkRequestPacket.Deserialize(
                    stream),
                NodePacketType.InstallSnapshotResponse => InstallSnapshotResponsePacket.Deserialize(stream),
                NodePacketType.InstallSnapshotRequest => InstallSnapshotRequestPacket.Deserialize(stream),
                NodePacketType.RetransmitRequest => RetransmitRequestPacket.Deserialize(stream),
                NodePacketType.InstallSnapshotChunkResponse => InstallSnapshotChunkResponsePacket.Deserialize(
                    stream),
            };
        }
        catch (SwitchExpressionException)
        {
            throw new UnknownPacketException((byte)marker);
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

        var packetType = (NodePacketType)marker;
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
                NodePacketType.ConnectRequest => await ConnectRequestPacket.DeserializeAsync(stream, token),
                NodePacketType.ConnectResponse => await ConnectResponsePacket.DeserializeAsync(stream, token),
                NodePacketType.InstallSnapshotChunkRequest => await InstallSnapshotChunkRequestPacket
                    .DeserializeAsync(stream, token),
                NodePacketType.InstallSnapshotResponse =>
                    await InstallSnapshotResponsePacket.DeserializeAsync(stream, token),
                NodePacketType.InstallSnapshotRequest =>
                    await InstallSnapshotRequestPacket.DeserializeAsync(stream, token),
                NodePacketType.RetransmitRequest =>
                    await RetransmitRequestPacket.DeserializeAsync(stream, token),
                NodePacketType.InstallSnapshotChunkResponse => await InstallSnapshotChunkResponsePacket
                    .DeserializeAsync(stream, token),
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

    protected static void VerifyCheckSum(Span<byte> payload)
    {
        var storedCheckSum = new SpanBinaryReader(payload[^4..]).ReadUInt32();
        var computedCheckSum = Crc32CheckSum.Compute(payload[..^4]);
        if (storedCheckSum != computedCheckSum)
        {
            throw new IntegrityException();
        }
    }
}