using System.Diagnostics;
using System.Runtime.CompilerServices;
using TaskFlux.Network.Client.Frontend.Exceptions;
using TaskFlux.Network.Requests;
using TaskFlux.Network.Requests.Packets;

namespace TaskFlux.Network.Client.Frontend;

internal abstract class BaseResponsePacketVisitor: IPacketVisitor
{
    private readonly PacketType _expectedType;

    public BaseResponsePacketVisitor(PacketType expectedType)
    {
        _expectedType = expectedType;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [StackTraceHidden]
    private void BaseVisit(Packet packet) => UnexpectedPacketResponseException.Throw(packet.Type, _expectedType);    

    public virtual void Visit(CommandRequestPacket packet)
    {
        BaseVisit(packet);
    }

    public virtual void Visit(CommandResponsePacket packet)
    {
        BaseVisit(packet);
    }

    public virtual void Visit(ErrorResponsePacket packet)
    {
        ErrorResponseException.Throw(packet.Message);
    }

    public virtual void Visit(NotLeaderPacket packet)
    {
        NotLeaderResponseException.Throw(packet.LeaderId);
    }

    public virtual void Visit(AuthorizationRequestPacket packet)
    {
        BaseVisit(packet);
    }

    public virtual void Visit(AuthorizationResponsePacket packet)
    {
        BaseVisit(packet);
    }

    public virtual void Visit(BootstrapRequestPacket packet)
    {
        BaseVisit(packet);
    }

    public virtual void Visit(BootstrapResponsePacket packet)
    {
        BaseVisit(packet);
    }
}