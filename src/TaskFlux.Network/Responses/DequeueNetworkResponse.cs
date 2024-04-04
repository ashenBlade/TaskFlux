using TaskFlux.Core.Queue;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Responses;

public sealed class DequeueNetworkResponse : NetworkResponse
{
    public override NetworkResponseType Type => NetworkResponseType.Dequeue;
    public QueueRecord? Record { get; }

    public DequeueNetworkResponse(QueueRecord? record)
    {
        Record = record;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var writer = new StreamBinaryWriter(stream);
        await writer.WriteAsync(( byte ) NetworkResponseType.Dequeue, token);
        if (Record is {Id: var id, Priority: var priority, Payload: var payload})
        {
            await writer.WriteAsync(true, token);
            await writer.WriteAsync(id, token);
            await writer.WriteAsync(priority, token);
            await writer.WriteBufferAsync(payload, token);
        }
        else
        {
            await writer.WriteAsync(false, token);
        }
    }

    public bool TryGetResponse(out QueueRecord record)
    {
        if (Record is { } r)
        {
            record = r;
            return true;
        }

        record = default;
        return false;
    }

    public new static async ValueTask<DequeueNetworkResponse> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var hasData = await reader.ReadBoolAsync(token);
        if (hasData)
        {
            var id = await reader.ReadUInt64Async(token);
            var priority = await reader.ReadInt64Async(token);
            var payload = await reader.ReadBufferAsync(token);
            return new DequeueNetworkResponse(new QueueRecord(new RecordId(id), priority, payload));
        }

        return new DequeueNetworkResponse(null);
    }

    public override T Accept<T>(INetworkResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}