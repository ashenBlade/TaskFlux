using System.Buffers;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Cluster.Network.Packets;

public class RequestVoteResponsePacket : NodePacket
{
    public RequestVoteResponse Response { get; }

    public RequestVoteResponsePacket(RequestVoteResponse response)
    {
        Response = response;
    }

    public override NodePacketType PacketType => NodePacketType.RequestVoteResponse;

    protected override int EstimatePacketSize()
    {
        return 1  // Маркер
             + 1  // Vote Granted
             + 4; // Current Term
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) NodePacketType.RequestVoteResponse);
        writer.Write(Response.VoteGranted);
        writer.Write(Response.CurrentTerm.Value);
    }

    public new static RequestVoteResponsePacket Deserialize(Stream stream)
    {
        const int packetSize = sizeof(bool) // Success 
                             + sizeof(int); // Term

        Span<byte> buffer = stackalloc byte[packetSize];
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    public new static async Task<RequestVoteResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        const int packetSize = sizeof(bool) // Success 
                             + sizeof(int); // Term

        var buffer = ArrayPool<byte>.Shared.Rent(packetSize);
        try
        {
            var memory = buffer.AsMemory(0, packetSize);
            await stream.ReadExactlyAsync(memory, token);
            return DeserializePayload(memory.Span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static RequestVoteResponsePacket DeserializePayload(Span<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer);
        var success = reader.ReadBoolean();
        var term = reader.ReadInt32();
        return new RequestVoteResponsePacket(new RequestVoteResponse(new Term(term), success));
    }
}