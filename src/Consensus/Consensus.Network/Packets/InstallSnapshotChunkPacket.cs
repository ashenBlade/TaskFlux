using TaskFlux.Serialization.Helpers;

namespace Consensus.Network.Packets;

public class InstallSnapshotChunkPacket : RaftPacket
{
    private readonly Memory<byte> _chunk;
    public override RaftPacketType PacketType => RaftPacketType.InstallSnapshotChunk;

    public InstallSnapshotChunkPacket(Memory<byte> chunk)
    {
        _chunk = chunk;
    }

    protected override int EstimatePacketSize()
    {
        return sizeof(RaftPacketType) // Маркер
             + sizeof(uint)           // Размер
             + _chunk.Length;         // Данные
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) RaftPacketType.InstallSnapshotChunk);
        writer.WriteBuffer(_chunk.Span);
    }
}