using System.Buffers;
using TaskFlux.Network.Responses;

namespace TaskFlux.Network.Packets;

public class CommandResponsePacket : Packet
{
    public NetworkResponse Response { get; }
    public override PacketType Type => PacketType.CommandResponse;

    public CommandResponsePacket(NetworkResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);
        Response = response;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        const int size = sizeof(PacketType);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            buffer[0] = ( byte ) PacketType.CommandResponse;
            await stream.WriteAsync(buffer.AsMemory(0, size), token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        await Response.SerializeAsync(stream, token);
    }

    public new static async Task<CommandResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var response = await NetworkResponse.DeserializeAsync(stream, token);
        return new CommandResponsePacket(response);
    }
}