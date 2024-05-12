using System.Buffers;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Responses;

public class CountNetworkResponse : NetworkResponse
{
    public int Count { get; }
    public override NetworkResponseType Type => NetworkResponseType.Count;

    public CountNetworkResponse(int count)
    {
        Count = count;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        const int size = sizeof(NetworkResponseType)
                         + sizeof(int);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var memory = buffer.AsMemory(0, size);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write((byte)NetworkResponseType.Count);
            writer.Write(Count);
            await stream.WriteAsync(memory, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public override T Accept<T>(INetworkResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }

    public new static async ValueTask<CountNetworkResponse> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var count = await reader.ReadInt32Async(token);
        return new CountNetworkResponse(count);
    }
}