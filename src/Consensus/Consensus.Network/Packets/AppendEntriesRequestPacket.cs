using Consensus.Core.Commands.AppendEntries;

namespace Consensus.Network.Packets;

public class AppendEntriesRequestPacket: IPacket
{
    public PacketType PacketType => PacketType.AppendEntriesRequest;
    public int EstimatePacketSize()
    {
        const int baseSize = 1  // Маркер
                           + 4  // Размер
                           + 4  // Leader Id
                           + 4  // LeaderCommit 
                           + 4  // Term
                           + 4  // PrevLogEntry Term
                           + 4  // PrevLogEntry Index
                           + 4; // Entries Count

        var entries = Request.Entries;
        if (entries.Count == 0)
        {
            return baseSize;
        }

        return baseSize + entries.Sum(entry => 4 // Term
                                             + 4 // Размер
                                             + entry.Data.Length);
    }

    public AppendEntriesRequest Request { get; }
    public AppendEntriesRequestPacket(AppendEntriesRequest request) 
    {
        Request = request;
    }
}