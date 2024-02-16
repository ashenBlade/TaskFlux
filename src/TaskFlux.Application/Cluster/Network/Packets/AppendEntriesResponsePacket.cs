using System.Buffers;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Cluster.Network.Packets;

public class AppendEntriesResponsePacket : NodePacket
{
    public override NodePacketType PacketType => NodePacketType.AppendEntriesResponse;
    public AppendEntriesResponse Response { get; }

    protected override int EstimatePayloadSize()
    {
        return SizeOf.Bool  // Success
             + SizeOf.Term; // Term
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(Response.Success);
        writer.Write(Response.Term);
    }

    public AppendEntriesResponsePacket(AppendEntriesResponse response)
    {
        Response = response;
    }

    private const int PayloadSize = SizeOf.Bool  // Success
                                  + SizeOf.Term; // Терм узла

    public new static AppendEntriesResponsePacket Deserialize(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[PayloadSize];
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    private static AppendEntriesResponsePacket DeserializePayload(Span<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer);
        var success = reader.ReadBool();
        var term = reader.ReadTerm();
        return new AppendEntriesResponsePacket(new AppendEntriesResponse(term, success));
    }

    public new static async Task<AppendEntriesResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
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
}