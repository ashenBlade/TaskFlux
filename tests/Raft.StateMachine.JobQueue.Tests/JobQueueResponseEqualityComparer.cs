using Raft.StateMachine.JobQueue.Commands;
using Raft.StateMachine.JobQueue.Commands.Batch;
using Raft.StateMachine.JobQueue.Commands.Dequeue;
using Raft.StateMachine.JobQueue.Commands.Enqueue;
using Raft.StateMachine.JobQueue.Commands.Error;
using Raft.StateMachine.JobQueue.Commands.GetCount;

namespace Raft.StateMachine.JobQueue.Tests;

public class JobQueueResponseEqualityComparer: IEqualityComparer<IJobQueueResponse>
{
    public static readonly JobQueueResponseEqualityComparer Instance = new();
    public bool Equals(IJobQueueResponse? x, IJobQueueResponse? y)
    {
        return x is not null && 
               y is not null && 
               Check(( dynamic ) x, ( dynamic ) y);
    }

    private bool Check(EnqueueResponse first, EnqueueResponse second) => first.Success == second.Success;
    private bool Check(DequeueResponse first, DequeueResponse second) =>
        ( first.Success, second.Success ) switch
        {
            (true, true)   => first.Key == second.Key && first.Payload.SequenceEqual(second.Payload),
            (false, false) => true,
            _              => false
        };

    private bool Check(GetCountResponse first, GetCountResponse second) => first.Count == second.Count;
    private bool Check(ErrorResponse first, ErrorResponse second) => first.Message == second.Message;
    private bool Check(BatchResponse first, BatchResponse second)
    {
        if (first.Responses.Count != second.Responses.Count)
        {
            return false;
        }

        foreach (var (x, y) in first.Responses.Zip(second.Responses))
        {
            if (!Check((dynamic)x, (dynamic)y))
            {
                return false;
            }
        }

        return true;
    }

    private bool Check(IJobQueueResponse _, IJobQueueResponse __) => false;

    public int GetHashCode(IJobQueueResponse obj)
    {
        return ( int ) obj.Type;
    }
}