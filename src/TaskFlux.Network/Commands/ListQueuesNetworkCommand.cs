using System.Buffers;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Commands;

public sealed class ListQueuesNetworkCommand : NetworkCommand
{
    public override NetworkCommandType Type => NetworkCommandType.ListQueues;

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        const int size = sizeof(NetworkCommandType);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var memory = buffer.AsMemory(0, size);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(NetworkCommandType.ListQueues);
            await stream.WriteAsync(memory, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public override T Accept<T>(INetworkCommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}