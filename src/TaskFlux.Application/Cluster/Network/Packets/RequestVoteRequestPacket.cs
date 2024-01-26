using System.Buffers;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Core;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Cluster.Network.Packets;

public class RequestVoteRequestPacket : NodePacket
{
    public RequestVoteRequest Request { get; }
    public override NodePacketType PacketType => NodePacketType.RequestVoteRequest;

    public RequestVoteRequestPacket(RequestVoteRequest request)
    {
        Request = request;
    }

    protected override int EstimatePacketSize()
    {
        return 1  // Маркер
             + 4  // Candidate Id
             + 4  // Candidate Term
             + 4  // LastLogEntry Term
             + 4; // LastLogEntry Index
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) NodePacketType.RequestVoteRequest);
        writer.Write(Request.CandidateId.Id);
        writer.Write(Request.CandidateTerm.Value);
        writer.Write(Request.LastLogEntryInfo.Term.Value);
        writer.Write(Request.LastLogEntryInfo.Index);
    }

    public new static RequestVoteRequestPacket Deserialize(Stream stream)
    {
        const int packetSize = sizeof(int)  // Id
                             + sizeof(int)  // Term
                             + sizeof(int)  // LogEntry Term
                             + sizeof(int); // LogEntry Index
        Span<byte> buffer = stackalloc byte[packetSize];
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    public new static async Task<RequestVoteRequestPacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        const int packetSize = sizeof(int)  // Id
                             + sizeof(int)  // Term
                             + sizeof(int)  // LogEntry Term
                             + sizeof(int); // LogEntry Index
        var buffer = ArrayPool<byte>.Shared.Rent(packetSize);
        try
        {
            var memory = buffer.AsMemory(0, packetSize);
            await stream.ReadExactlyAsync(memory, token);
            return DeserializePayload(buffer);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static RequestVoteRequestPacket DeserializePayload(Span<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer);
        var id = reader.ReadInt32();
        var term = reader.ReadInt32();
        var entryTerm = reader.ReadInt32();
        var entryIndex = reader.ReadInt32();
        return new RequestVoteRequestPacket(new RequestVoteRequest(new NodeId(id), new Term(term),
            new LogEntryInfo(new Term(entryTerm), entryIndex)));
    }
}