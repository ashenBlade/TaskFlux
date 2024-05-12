using TaskFlux.Core;

namespace TaskFlux.Network.Responses;

public interface ITaskQueueInfo
{
    public QueueName QueueName { get; }
    public int Count { get; }
    public Dictionary<string, string> Policies { get; }
}