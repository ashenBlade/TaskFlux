using Utils.CheckSum;
using Utils.Serialization;

namespace Consensus.Network.Packets;

public class InstallSnapshotChunkRequestPacket : RaftPacket
{
    internal const int DataStartPosition = 1  // Маркер
                                         + 4; // Размер чанка

    public ReadOnlyMemory<byte> Chunk { get; }
    public override RaftPacketType PacketType => RaftPacketType.InstallSnapshotChunkRequest;

    public InstallSnapshotChunkRequestPacket(ReadOnlyMemory<byte> chunk)
    {
        Chunk = chunk;
    }

    protected override int EstimatePacketSize()
    {
        return sizeof(RaftPacketType) // Маркер
             + sizeof(int)            // Размер
             + Chunk.Length           // Данные
             + sizeof(uint);          // Чек-сумма
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var checkSum = Crc32CheckSum.Compute(Chunk.Span);
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) RaftPacketType.InstallSnapshotChunkRequest);
        writer.WriteBuffer(Chunk.Span);
        writer.Write(checkSum);
    }

    internal int GetDataEndPosition()
    {
        return 1             // Маркер
             + 4             // Размер данных
             + Chunk.Length; // Сами данные
    }
}