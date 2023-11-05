using TaskFlux.Abstractions;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;

namespace TaskFlux.Commands.Serialization.Tests;

// ReSharper disable UnusedParameter.Local
public class ResultEqualityComparer : IEqualityComparer<Response>
{
    public static readonly ResultEqualityComparer Instance = new();

    public bool Equals(Response? x, Response? y)
    {
        return Check(( dynamic ) x!, ( dynamic ) y!);
    }

    private static bool Check(CountResponse first, CountResponse second) => first.Count == second.Count;
    private static bool Check(EnqueueResponse first, EnqueueResponse second) => first.Success == second.Success;

    private static bool Check(DequeueResponse first, DequeueResponse second)
    {
        var firstOk = first.TryGetResult(out var k1, out var p1);
        var secondOk = second.TryGetResult(out var k2, out var p2);
        return ( firstOk, secondOk ) switch
               {
                   (true, true)   => k1 == k2 && p1.SequenceEqual(p2),
                   (false, false) => true,
                   _              => false
               };
    }

    private static bool Check(ErrorResponse first, ErrorResponse second) =>
        first.ErrorType == second.ErrorType && first.Message == second.Message;

    private static bool Check(OkResponse first, OkResponse second) => true;

    private static bool Check(ListQueuesResponse first, ListQueuesResponse second) =>
        first.Metadata.SequenceEqual(second.Metadata, TaskQueueMetadataEqualityComparer.Instance);

    private class TaskQueueMetadataEqualityComparer : IEqualityComparer<ITaskQueueMetadata>
    {
        public static TaskQueueMetadataEqualityComparer Instance = new();

        public bool Equals(ITaskQueueMetadata? x, ITaskQueueMetadata? y)
        {
            if (x is null || y is null)
            {
                return y is null && x is null;
            }

            return x.Count == y.Count && x.QueueName == y.QueueName && x.MaxSize == y.MaxSize;
        }

        public int GetHashCode(ITaskQueueMetadata obj)
        {
            return HashCode.Combine(obj.QueueName, obj.Count, obj.MaxSize);
        }
    }

    public int GetHashCode(Response obj)
    {
        return ( int ) obj.Type;
    }
}