using System.Buffers;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Packets;

public class NotLeaderPacket : Packet
{
    public int? LeaderId { get; }

    public NotLeaderPacket(int? leaderId)
    {
        LeaderId = leaderId;
    }

    public override PacketType Type => PacketType.NotLeader;


    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        const int estimatedSize = sizeof(PacketType)
                                + sizeof(int);
        var array = ArrayPool<byte>.Shared.Rent(estimatedSize);
        try
        {
            var buffer = array.AsMemory(0, estimatedSize);
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write(( byte ) PacketType.NotLeader);
            writer.Write(LeaderId ?? -1);
            await stream.WriteAsync(buffer, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(array);
        }
    }

    public new static async Task<NotLeaderPacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        int? leaderId = await reader.ReadInt32Async(token);
        if (leaderId == -1)
        {
            leaderId = null;
        }

        return new NotLeaderPacket(leaderId);
    }
}