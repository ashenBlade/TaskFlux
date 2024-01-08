using Consensus.Peer.Exceptions;
using Utils.CheckSum;
using Utils.Serialization;

namespace Consensus.Network.Packets;

public class InstallSnapshotChunkRequestPacket : NodePacket
{
    internal const int DataStartPosition = 1  // Маркер
                                         + 4; // Размер чанка

    public ReadOnlyMemory<byte> Chunk { get; }
    public override NodePacketType PacketType => NodePacketType.InstallSnapshotChunkRequest;

    public InstallSnapshotChunkRequestPacket(ReadOnlyMemory<byte> chunk)
    {
        Chunk = chunk;
    }

    protected override int EstimatePacketSize()
    {
        return sizeof(NodePacketType) // Маркер
             + sizeof(int)            // Размер
             + Chunk.Length           // Данные
             + sizeof(uint);          // Чек-сумма
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var checkSum = Crc32CheckSum.Compute(Chunk.Span);
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) NodePacketType.InstallSnapshotChunkRequest);
        writer.WriteBuffer(Chunk.Span);
        writer.Write(checkSum);
    }

    internal int GetDataEndPosition()
    {
        return 1             // Маркер
             + 4             // Размер данных
             + Chunk.Length; // Сами данные
    }

    public new static InstallSnapshotChunkRequestPacket Deserialize(Stream stream)
    {
        // Читаем размер данных
        var reader = new StreamBinaryReader(stream);
        var payloadLength = reader.ReadInt32();
        var totalPayloadSize = payloadLength + sizeof(uint);
        using var buffer = Rent(totalPayloadSize);
        stream.ReadExactly(buffer.GetSpan());
        return DeserializeDataVerifyCheckSum(buffer.GetSpan());
    }

    public new static async Task<InstallSnapshotChunkRequestPacket> DeserializeAsync(
        Stream stream,
        CancellationToken token)
    {
        // Читаем размер данных
        var reader = new StreamBinaryReader(stream);
        var payloadLength = await reader.ReadInt32Async(token);
        var totalPayloadSize = payloadLength + sizeof(uint);
        using var buffer = Rent(totalPayloadSize);
        await stream.ReadExactlyAsync(buffer.GetMemory(), token);
        return DeserializeDataVerifyCheckSum(buffer.GetSpan());
    }

    private static InstallSnapshotChunkRequestPacket DeserializeDataVerifyCheckSum(Span<byte> payload)
    {
        ValidateChecksum(payload);

        if (payload.Length == 4)
        {
            return new InstallSnapshotChunkRequestPacket(Array.Empty<byte>());
        }

        return new InstallSnapshotChunkRequestPacket(payload[..^4].ToArray());

        static void ValidateChecksum(Span<byte> buffer)
        {
            var crcReader = new SpanBinaryReader(buffer[^4..]);
            var storedCrc = crcReader.ReadUInt32();
            var calculatedCrc = Crc32CheckSum.Compute(buffer[..^4]);
            if (storedCrc != calculatedCrc)
            {
                throw new IntegrityException();
            }
        }
    }
}