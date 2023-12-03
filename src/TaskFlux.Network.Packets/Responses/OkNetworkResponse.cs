using System.Buffers;

namespace TaskFlux.Network.Packets.Responses;

public class OkNetworkResponse : NetworkResponse
{
    public static readonly OkNetworkResponse Instance = new();
    public override NetworkResponseType Type => NetworkResponseType.Ok;

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var length = sizeof(NetworkResponseType);
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var memory = buffer.AsMemory(0, length);
            buffer[0] = ( byte ) NetworkResponseType.Ok;
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
}