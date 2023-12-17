using System.Buffers;
using TaskFlux.Network.Commands;

namespace TaskFlux.Network.Packets;

public class CommandRequestPacket : Packet
{
    public NetworkCommand Command { get; }
    public override PacketType Type => PacketType.CommandRequest;

    public CommandRequestPacket(NetworkCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        Command = command;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        const int size = sizeof(PacketType);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            buffer[0] = ( byte ) PacketType.CommandRequest;
            await stream.WriteAsync(buffer.AsMemory(0, size), token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        await Command.SerializeAsync(stream, token);
    }

    public new static async Task<CommandRequestPacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var command = await NetworkCommand.DeserializeAsync(stream, token);
        return new CommandRequestPacket(command);
    }
}