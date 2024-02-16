using System.Buffers;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Cluster.Network.Packets;

public class RequestVoteResponsePacket : NodePacket
{
    public RequestVoteResponse Response { get; }

    public RequestVoteResponsePacket(RequestVoteResponse response)
    {
        Response = response;
    }

    public override NodePacketType PacketType => NodePacketType.RequestVoteResponse;

    protected override int EstimatePayloadSize()
    {
        return PayloadSize;
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(Response.VoteGranted);
        writer.Write(Response.CurrentTerm.Value);
    }

    // TODO: поменять местами
    private const int PayloadSize = SizeOf.Bool
                                  + SizeOf.Term;

    public new static RequestVoteResponsePacket Deserialize(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[PayloadSize];
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    public new static async Task<RequestVoteResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(PayloadSize);
        try
        {
            var memory = buffer.AsMemory(0, PayloadSize);
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
        var term = reader.ReadTerm();
        return new RequestVoteResponsePacket(new RequestVoteResponse(term, success));
    }
}