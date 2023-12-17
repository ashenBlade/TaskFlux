using TaskFlux.Models;

namespace TaskFlux.Network.Responses;

public interface ITaskQueueInfo
{
    public QueueName QueueName { get; }
    public int Count { get; }
    public Dictionary<string, string> Policies { get; }
}