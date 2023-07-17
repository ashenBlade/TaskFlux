using Consensus.StateMachine.JobQueue.Commands;
using Consensus.StateMachine.JobQueue.Commands.Batch;
using Consensus.StateMachine.JobQueue.Commands.Dequeue;
using Consensus.StateMachine.JobQueue.Commands.Enqueue;
using Consensus.StateMachine.JobQueue.Commands.GetCount;

namespace Consensus.StateMachine.JobQueue.Tests;

public class JobQueueRequestEqualityComparer: IEqualityComparer<IJobQueueRequest>
{
    public static readonly JobQueueRequestEqualityComparer Instance = new();
    public bool Equals(IJobQueueRequest? x, IJobQueueRequest? y)
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

    private bool CheckInner(IJobQueueRequest _, IJobQueueRequest __) => false;
    
    public int GetHashCode(IJobQueueRequest obj)
    {
        return ( int ) obj.Type;
    }
}