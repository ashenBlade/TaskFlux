using TaskFlux.Serialization.Helpers;

namespace Consensus.Network.Packets;

public class InstallSnapshotChunkPacket : RaftPacket
{
    public ReadOnlyMemory<byte> Chunk { get; }
    public override RaftPacketType PacketType => RaftPacketType.InstallSnapshotChunk;

    public InstallSnapshotChunkPacket(ReadOnlyMemory<byte> chunk)
    {
        Chunk = chunk;
    }

    protected override int EstimatePacketSize()
    {
        return sizeof(RaftPacketType) // Маркер
             + sizeof(uint)           // Размер
             + Chunk.Length;          // Данные
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) RaftPacketType.InstallSnapshotChunk);
        writer.WriteBuffer(Chunk.Span);
    }
}