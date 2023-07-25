using TaskFlux.Network.Requests.Packets;

namespace TaskFlux.Network.Requests.Serialization.Tests;

public class PacketEqualityComparer: IEqualityComparer<Packet>
{
    public static readonly PacketEqualityComparer Instance = new();
    public bool Equals(Packet x, Packet y)
    {
        return Check(( dynamic ) x, ( dynamic ) y);
    }

    private bool Check(CommandRequestPacket first, CommandRequestPacket second) =>
        first.Payload.SequenceEqual(second.Payload);

    private bool Check(CommandResponsePacket first, CommandResponsePacket second) =>
        first.Payload.SequenceEqual(second.Payload);

    private bool Check(ErrorResponsePacket first, ErrorResponsePacket second) => 
        first.Message == second.Message;

    private bool Check(NotLeaderPacket first, NotLeaderPacket second) => 
        first.LeaderId == second.LeaderId;

    public int GetHashCode(Packet obj)
    {
        return ( int ) obj.Type;
    }
}