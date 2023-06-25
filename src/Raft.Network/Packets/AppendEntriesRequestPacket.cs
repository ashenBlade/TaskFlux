using Raft.Core.Commands.AppendEntries;

namespace Raft.Network.Packets;

public class AppendEntriesRequestPacket: IPacket
{
    public PacketType PacketType => PacketType.AppendEntriesRequest;
    public AppendEntriesRequest Request { get; }
    public AppendEntriesRequestPacket(AppendEntriesRequest request) 
    {
        Request = request;
    }
}