using System.Buffers;
using Consensus.Core.Commands.AppendEntries;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Network.Packets;

public class AppendEntriesRequestPacket: RaftPacket
{
    public override RaftPacketType PacketType => RaftPacketType.AppendEntriesRequest;
    protected override int EstimatePacketSize()
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

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write((byte)RaftPacketType.AppendEntriesRequest);
        writer.Write(buffer.Length - 
                     (
                         sizeof(RaftPacketType) // Packet Type 
                       + sizeof(int)            // Length
                     )
            );

        writer.Write(Request.Term.Value);
        writer.Write(Request.LeaderId.Value);
        writer.Write(Request.LeaderCommit);
        writer.Write(Request.PrevLogEntryInfo.Term.Value);
        writer.Write(Request.PrevLogEntryInfo.Index);
        writer.Write(Request.Entries.Count);
        if (Request.Entries.Count == 0)
        {
            return;
        }
        
        foreach (var entry in Request.Entries)
        {
            writer.Write(entry.Term.Value);
            writer.WriteBuffer(entry.Data);
        }
    }

    public AppendEntriesRequest Request { get; }
    public AppendEntriesRequestPacket(AppendEntriesRequest request) 
    {
        Request = request;
    }
}