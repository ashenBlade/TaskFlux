using Raft.Core.Commands.AppendEntries;

namespace Raft.Network.Packets;

public class AppendEntriesResponsePacket: IPacket
{
    public PacketType PacketType => PacketType.AppendEntriesResponse;
    public AppendEntriesResponse Response { get; }
    public AppendEntriesResponsePacket(AppendEntriesResponse response)
    {
        Response = response;
    }
}