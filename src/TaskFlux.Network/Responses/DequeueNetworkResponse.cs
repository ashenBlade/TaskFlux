using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Responses;

public sealed class DequeueNetworkResponse : NetworkResponse
{
    public override NetworkResponseType Type => NetworkResponseType.Dequeue;
    public (long, byte[])? Data { get; }

    public DequeueNetworkResponse((long, byte[])? data)
    {
        Data = data;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var writer = new StreamBinaryWriter(stream);
        await writer.WriteAsync(( byte ) NetworkResponseType.Dequeue, token);
        if (Data is var (key, data))
        {
            await writer.WriteAsync(true, token);
            await writer.WriteAsync(key, token);
            await writer.WriteBufferAsync(data, token);
        }
        else
        {
            await writer.WriteAsync(false, token);
        }
    }

    public bool TryGetResponse(out long key, out byte[] data)
    {
        if (Data is var (k, d))
        {
            key = k;
            data = d;
            return true;
        }

        key = default!;
        data = default!;
        return false;
    }

    public new static async ValueTask<DequeueNetworkResponse> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var hasData = await reader.ReadBoolAsync(token);
        if (hasData)
        {
            var key = await reader.ReadInt64Async(token);
            var data = await reader.ReadBufferAsync(token);
            return new DequeueNetworkResponse(( key, data ));
        }

        return new DequeueNetworkResponse(null);
    }

    public override T Accept<T>(INetworkResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}