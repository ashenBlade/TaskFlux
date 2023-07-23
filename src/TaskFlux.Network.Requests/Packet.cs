using System.Security.Cryptography;

namespace TaskFlux.Network.Requests;

public abstract class Packet
{
    protected internal Packet() 
    {  }
    public abstract PacketType Type { get; }
    public abstract void Accept(IPacketVisitor visitor);
    public abstract ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default);
}