using Consensus.Core.Commands.AppendEntries;

namespace Consensus.Network.Packets;

public class AppendEntriesResponsePacket: IPacket
{
    public PacketType PacketType => PacketType.AppendEntriesResponse;
    public int EstimatePacketSize()
    {
        return 1  // Маркер
             + 4  // Длина
             + 1  // Success
             + 4; // Term
    }

    public AppendEntriesResponse Response { get; }
    public AppendEntriesResponsePacket(AppendEntriesResponse response)
    {
        Response = response;
    }
}