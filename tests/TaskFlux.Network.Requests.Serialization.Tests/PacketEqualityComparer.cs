using TaskFlux.Network.Requests.Authorization;
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

    private bool Check(AuthorizationResponsePacket first, AuthorizationResponsePacket second) =>
        ( first.Success, second.Success ) switch
        {
            (true, true)   => true,
            (false, false) => first.ErrorReason == second.ErrorReason,
            _              => false
        };

    private bool Check(AuthorizationRequestPacket first, AuthorizationRequestPacket second)
    {
        return CheckAuth((dynamic) first.AuthorizationMethod, (dynamic) second.AuthorizationMethod);
    }

    private bool CheckAuth(NoneAuthorizationMethod first, NoneAuthorizationMethod second) => true;
    
    public int GetHashCode(Packet obj)
    {
        return ( int ) obj.Type;
    }
}