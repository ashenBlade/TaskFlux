using System.Runtime.CompilerServices;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Network.Packets;

public class ConnectResponsePacket: RaftPacket
{
    public override RaftPacketType PacketType => RaftPacketType.ConnectResponse;
    public bool Success { get; }

    public ConnectResponsePacket(bool success) 
    {
        Success = success;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected override int EstimatePacketSize()
    {
        return sizeof(RaftPacketType) // Маркер
             + sizeof(bool);          // Success
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write((byte)RaftPacketType.ConnectResponse);
        writer.Write(Success);
    }
}