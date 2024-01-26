using TaskFlux.Network.Authorization;
using TaskFlux.Network.Packets;

namespace TaskFlux.Network.Tests;

// ReSharper disable UnusedParameter.Local
public class PacketEqualityComparer : IEqualityComparer<Packet>
{
    public static readonly PacketEqualityComparer Instance = new();

    public bool Equals(Packet? x, Packet? y)
    {
        return x is not null
            && y is not null
            && Check(( dynamic ) x, ( dynamic ) y);
    }

    private bool Check(CommandRequestPacket first, CommandRequestPacket second) =>
        NetworkCommandEqualityComparer.Instance.Equals(first.Command, second.Command);

    private bool Check(CommandResponsePacket first, CommandResponsePacket second) =>
        NetworkResponseEqualityComparer.Instance.Equals(first.Response, second.Response);

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

    private bool Check(BootstrapRequestPacket first, BootstrapRequestPacket second) =>
        ( first.Major, first.Minor, first.Patch ) == ( second.Major, second.Minor, second.Patch );

    private bool Check(BootstrapResponsePacket first, BootstrapResponsePacket second)
        => ( first.Success, second.Success ) switch
           {
               (true, true)   => true,
               (false, false) => first.Reason == second.Reason,
               _              => false
           };

    private bool Check(AcknowledgeRequestPacket first, AcknowledgeRequestPacket second) => true;
    private bool Check(NegativeAcknowledgeRequestPacket first, NegativeAcknowledgeRequestPacket second) => true;

    private bool Check(ClusterMetadataRequestPacket first, ClusterMetadataRequestPacket second)
    {
        return true;
    }

    private bool Check(ClusterMetadataResponsePacket first, ClusterMetadataResponsePacket second)
        => first.EndPoints.SequenceEqual(second.EndPoints)
        && first.LeaderId == second.LeaderId
        && first.RespondingId == second.RespondingId;

    private bool Check(AuthorizationRequestPacket first, AuthorizationRequestPacket second)
    {
        return CheckAuth(( dynamic ) first.AuthorizationMethod, ( dynamic ) second.AuthorizationMethod);
    }

    private bool CheckAuth(NoneAuthorizationMethod first, NoneAuthorizationMethod second) => true;
    private bool Check(OkPacket first, OkPacket second) => true;

    public int GetHashCode(Packet obj)
    {
        return ( int ) obj.Type;
    }
}