using TaskFlux.Consensus.Cluster.Network.Exceptions;
using TaskFlux.Utils.CheckSum;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Cluster.Network.Packets;

public class InstallSnapshotChunkRequestPacket : NodePacket
{
    internal const int DataStartPosition = SizeOf.ArrayLength; // Размер чанка
    private const int DataAlignment = 4;

    public ReadOnlyMemory<byte> Chunk { get; }
    public override NodePacketType PacketType => NodePacketType.InstallSnapshotChunkRequest;

    public InstallSnapshotChunkRequestPacket(ReadOnlyMemory<byte> chunk)
    {
        Chunk = chunk;
    }

    protected override int EstimatePayloadSize()
    {
        return SizeOf.BufferAligned(Chunk.Span, DataAlignment); // Данные
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.WriteBufferAligned(Chunk.Span, DataAlignment);
    }

    internal int GetDataEndPosition()
    {
        return SizeOf.PacketType
             + SizeOf.BufferAligned(Chunk.Span, DataAlignment)
             + SizeOf.CheckSum;
    }

    public new static InstallSnapshotChunkRequestPacket Deserialize(Stream stream)
    {
        // Читаем размер данных
        Span<byte> lengthSpan = stackalloc byte[sizeof(int)];
        stream.ReadExactly(lengthSpan);
        var bufferLength = new SpanBinaryReader(lengthSpan).ReadInt32();
        var restPayloadSize = bufferLength + GetAlignment(bufferLength) + SizeOf.CheckSum;
        if (restPayloadSize < 1024) // 1 Кб
        {
            Span<byte> payloadBuffer = stackalloc byte[restPayloadSize];
            stream.ReadExactly(payloadBuffer);
            return DeserializePayload(payloadBuffer, lengthSpan, bufferLength);
        }

        using var buffer = Rent(restPayloadSize);
        stream.ReadExactly(buffer.GetSpan());
        return DeserializePayload(buffer.GetSpan(), lengthSpan, bufferLength);
    }

    private static int GetAlignment(int payloadLength)
    {
        return payloadLength % DataAlignment;
    }

    public new static async Task<InstallSnapshotChunkRequestPacket> DeserializeAsync(
        Stream stream,
        CancellationToken token)
    {
        // Читаем размер данных
        using var lengthBuffer = Rent(sizeof(int));
        await stream.ReadExactlyAsync(lengthBuffer.GetMemory(), token);
        var bufferLength = new SpanBinaryReader(lengthBuffer.GetSpan()).ReadInt32();
        var totalPayloadSize = bufferLength + GetAlignment(bufferLength) + sizeof(uint);

        using var buffer = Rent(totalPayloadSize);
        await stream.ReadExactlyAsync(buffer.GetMemory(), token);
        return DeserializePayload(buffer.GetSpan(), lengthBuffer.GetSpan(), bufferLength);
    }

    private static InstallSnapshotChunkRequestPacket DeserializePayload(
        Span<byte> restPayload,
        Span<byte> lengthSpan,
        int bufferLength)
    {
        ValidateChecksum(restPayload, lengthSpan);

        if (bufferLength == 0)
        {
            return new InstallSnapshotChunkRequestPacket(Array.Empty<byte>());
        }

        var alignment = GetAlignment(bufferLength);
        return new InstallSnapshotChunkRequestPacket(restPayload[..^( alignment + sizeof(uint) )].ToArray());

        static void ValidateChecksum(Span<byte> restPayload, Span<byte> lengthPayload)
        {
            var storedCrc = new SpanBinaryReader(restPayload[^4..]).ReadUInt32();
            var calculatedCrc = Crc32CheckSum.Compute(lengthPayload);
            calculatedCrc = Crc32CheckSum.Compute(calculatedCrc, restPayload[..^4]);
            if (storedCrc != calculatedCrc)
            {
                throw new IntegrityException();
            }
        }
    }
}