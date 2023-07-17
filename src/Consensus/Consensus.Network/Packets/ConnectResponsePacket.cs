namespace Consensus.Network.Packets;

public class ConnectResponsePacket: IPacket
{
    public PacketType PacketType => PacketType.ConnectResponse;
    public bool Success { get; }

    public ConnectResponsePacket(bool success) 
    {
        Success = success;
    }
}