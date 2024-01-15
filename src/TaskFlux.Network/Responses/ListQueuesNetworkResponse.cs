using TaskFlux.Core;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Responses;

public sealed class ListQueuesNetworkResponse : NetworkResponse
{
    public IReadOnlyCollection<ITaskQueueInfo> Queues { get; }
    public override NetworkResponseType Type => NetworkResponseType.ListQueues;

    public ListQueuesNetworkResponse(IReadOnlyCollection<ITaskQueueInfo> queues)
    {
        Queues = queues;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var writer = new StreamBinaryWriter(stream);
        await writer.WriteAsync(( byte ) NetworkResponseType.ListQueues, token);
        await writer.WriteAsync(Queues.Count, token);
        if (Queues.Count > 0)
        {
            foreach (var queue in Queues)
            {
                await writer.WriteAsync(queue.QueueName, token);
                await writer.WriteAsync(queue.Count, token);
                await writer.WriteAsync(queue.Policies.Count, token);
                if (queue.Policies.Count > 0)
                {
                    foreach (var (key, value) in queue.Policies)
                    {
                        await writer.WriteAsync(key, token);
                        await writer.WriteAsync(value, token);
                    }
                }
            }
        }
    }

    public new static async Task<ListQueuesNetworkResponse> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var queuesCount = await reader.ReadInt32Async(token);
        if (queuesCount == 0)
        {
            return new ListQueuesNetworkResponse(Array.Empty<ITaskQueueInfo>());
        }

        var queues = new ITaskQueueInfo[queuesCount];
        for (int i = 0; i < queuesCount; i++)
        {
            var name = await reader.ReadQueueNameAsync(token);
            var count = await reader.ReadInt32Async(token);
            var policiesCount = await reader.ReadInt32Async(token);
            var policies = new Dictionary<string, string>(policiesCount);
            for (int j = 0; j < policiesCount; j++)
            {
                var key = await reader.ReadStringAsync(token);
                var value = await reader.ReadStringAsync(token);
                policies[key] = value;
            }

            queues[i] = new NetworkDeserializedTaskQueueInfo(name, count, policies);
        }

        return new ListQueuesNetworkResponse(queues);
    }

    private class NetworkDeserializedTaskQueueInfo : ITaskQueueInfo
    {
        public NetworkDeserializedTaskQueueInfo(QueueName queueName, int count, Dictionary<string, string> policies)
        {
            QueueName = queueName;
            Count = count;
            Policies = policies;
        }

        public QueueName QueueName { get; }
        public int Count { get; }
        public Dictionary<string, string> Policies { get; }
    }

    public override T Accept<T>(INetworkResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}