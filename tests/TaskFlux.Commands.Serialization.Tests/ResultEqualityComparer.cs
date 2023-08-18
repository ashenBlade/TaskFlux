using JobQueue.Core;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;

namespace TaskFlux.Commands.Serialization.Tests;

public class ResultEqualityComparer: IEqualityComparer<Result>
{
    public static readonly ResultEqualityComparer Instance = new();
    public bool Equals(Result? x, Result? y)
    {
        return Check(( dynamic ) x!, ( dynamic ) y!);
    }

    private static bool Check(CountResult first, CountResult second) => first.Count == second.Count;
    private static bool Check(EnqueueResult first, EnqueueResult second) => first.Success == second.Success;
    private static bool Check(DequeueResult first, DequeueResult second)
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

    private static bool Check(ErrorResult first, ErrorResult second) =>
        first.ErrorType == second.ErrorType && 
        first.Message == second.Message;

    private static bool Check(OkResult first, OkResult second) => true;

    private static bool Check(ListQueuesResult first, ListQueuesResult second) =>
        first.Metadata.SequenceEqual(second.Metadata, JobQueueMetadataEqualityComparer.Instance);

    private class JobQueueMetadataEqualityComparer : IEqualityComparer<IJobQueueMetadata>
    {
        public static JobQueueMetadataEqualityComparer Instance = new();
        public bool Equals(IJobQueueMetadata? x, IJobQueueMetadata? y)
        {
            if (x is null || y is null)
            {
                return y is null && x is null;
            }

            return x.Count == y.Count && 
                   x.QueueName == y.QueueName && 
                   x.MaxSize == y.MaxSize;
        }

        public int GetHashCode(IJobQueueMetadata obj)
        {
            throw new NotImplementedException();
        }
    }

    public int GetHashCode(Result obj)
    {
        return ( int ) obj.Type;
    }
}