using System.Buffers;
using TaskFlux.Consensus.Cluster;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Network.Message.Packets;

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
        writer.Write(Response.CurrentTerm.Value);
        writer.Write(Response.VoteGranted);
    }

    private const int PayloadSize = SizeOf.Term
                                    + SizeOf.Bool;

    public new static RequestVoteResponsePacket Deserialize(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[PayloadSize + sizeof(uint)];
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    public new static async Task<RequestVoteResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(PayloadSize + sizeof(uint));
        try
        {
            var memory = buffer.AsMemory(0, PayloadSize + sizeof(uint));
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
        VerifyCheckSum(buffer);
        var reader = new SpanBinaryReader(buffer);
        var term = reader.ReadTerm();
        var success = reader.ReadBoolean();
        return new RequestVoteResponsePacket(new RequestVoteResponse(term, success));
    }
}