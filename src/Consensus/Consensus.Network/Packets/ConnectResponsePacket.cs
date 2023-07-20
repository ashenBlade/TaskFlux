using System.Runtime.CompilerServices;

namespace Consensus.Network.Packets;

public class ConnectResponsePacket: IPacket
{
    public PacketType PacketType => PacketType.ConnectResponse;
    public bool Success { get; }

    public ConnectResponsePacket(bool success) 
    {
        Success = success;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int EstimatePacketSize()
    {
        return 1  // Маркер
             + 4  // Размер
             + 1; // Success
    }
}