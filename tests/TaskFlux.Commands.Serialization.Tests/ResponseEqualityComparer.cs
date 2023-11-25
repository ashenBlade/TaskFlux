using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.PolicyViolation;
using TaskFlux.Core.Policies;
using TaskFlux.Core.Queue;

namespace TaskFlux.Commands.Serialization.Tests;

// ReSharper disable UnusedParameter.Local
public class ResponseEqualityComparer : IEqualityComparer<Response>
{
    public static readonly ResponseEqualityComparer Instance = new();

    public bool Equals(Response? x, Response? y)
    {
        return Check(( dynamic ) x!, ( dynamic ) y!);
    }

    private static bool Check(CountResponse first, CountResponse second) => first.Count == second.Count;

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

    private static bool Check(PolicyViolationResponse left, PolicyViolationResponse right)
    {
        return CheckPolicy(( dynamic ) left.ViolatedPolicy, ( dynamic ) right.ViolatedPolicy);
    }

    private static bool CheckPolicy(MaxQueueSizeQueuePolicy left, MaxQueueSizeQueuePolicy right) =>
        left.MaxQueueSize == right.MaxQueueSize;

    private static bool CheckPolicy(MaxPayloadSizeQueuePolicy left, MaxPayloadSizeQueuePolicy right) =>
        left.MaxPayloadSize == right.MaxPayloadSize;

    private static bool CheckPolicy(PriorityRangeQueuePolicy left, PriorityRangeQueuePolicy right) =>
        ( left.Min, left.Max ) == ( right.Min, right.Max );

    private class TaskQueueMetadataEqualityComparer : IEqualityComparer<ITaskQueueMetadata>
    {
        public static TaskQueueMetadataEqualityComparer Instance = new();

        public bool Equals(ITaskQueueMetadata? x, ITaskQueueMetadata? y)
        {
            if (x is null || y is null)
            {
                return y is null && x is null;
            }

            return x.Count == y.Count && x.QueueName == y.QueueName && x.MaxQueueSize == y.MaxQueueSize;
        }

        public int GetHashCode(ITaskQueueMetadata obj)
        {
            return HashCode.Combine(obj.QueueName, obj.Count, obj.MaxQueueSize);
        }
    }

    public int GetHashCode(Response obj)
    {
        return ( int ) obj.Type;
    }
}