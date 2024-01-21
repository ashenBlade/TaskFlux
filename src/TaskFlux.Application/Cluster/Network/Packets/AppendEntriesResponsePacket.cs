using System.Buffers;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Cluster.Network.Packets;

public class AppendEntriesResponsePacket : NodePacket
{
    public override NodePacketType PacketType => NodePacketType.AppendEntriesResponse;

    protected override int EstimatePacketSize()
    {
        return 1  // Маркер
             + 1  // Success
             + 4; // Term
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) NodePacketType.AppendEntriesResponse);
        writer.Write(Response.Success);
        writer.Write(Response.Term.Value);
    }


    public AppendEntriesResponse Response { get; }

    public AppendEntriesResponsePacket(AppendEntriesResponse response)
    {
        Response = response;
    }

    public new static AppendEntriesResponsePacket Deserialize(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[sizeof(bool) + sizeof(int)];
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    private static AppendEntriesResponsePacket DeserializePayload(Span<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer);
        var success = reader.ReadBool();
        var term = reader.ReadInt32();
        return new AppendEntriesResponsePacket(new AppendEntriesResponse(new Term(term), success));
    }

    public new static async Task<AppendEntriesResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        const int packetSize = sizeof(bool)
                             + sizeof(int);
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
}