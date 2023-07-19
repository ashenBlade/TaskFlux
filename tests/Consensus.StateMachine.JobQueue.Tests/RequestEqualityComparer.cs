using TaskFlux.Requests;
using TaskFlux.Requests.Batch;
using TaskFlux.Requests.Dequeue;
using TaskFlux.Requests.GetCount;
using TaskFlux.Requests.Requests.JobQueue.Enqueue;

namespace Consensus.StateMachine.JobQueue.Tests;

public class RequestEqualityComparer: IEqualityComparer<IRequest>
{
    public static readonly RequestEqualityComparer Instance = new();
    public bool Equals(IRequest? x, IRequest? y)
    {

        return CheckInner(( dynamic ) x!, ( dynamic ) y!);
    }

    private bool CheckInner(EnqueueRequest first, EnqueueRequest second) =>
        first.Key == second.Key && first.Payload.SequenceEqual(second.Payload);
    private bool CheckInner(DequeueRequest _, DequeueRequest __) => true;
    private bool CheckInner(GetCountRequest _, GetCountRequest __) => true;
    private bool CheckInner(BatchRequest first, BatchRequest second)
    {
        if (first.Requests.Count != second.Requests.Count)
        {
            return false;
        }

        foreach (var (one, two) in first.Requests.Zip(second.Requests))
        {
            if (!CheckInner((dynamic) one, (dynamic) two))
            {
                return false;
            }
        }

        return true;
    }

    private bool CheckInner(IRequest _, IRequest __) => false;
    
    public int GetHashCode(IRequest obj)
    {
        return obj.GetHashCode();
    }
}