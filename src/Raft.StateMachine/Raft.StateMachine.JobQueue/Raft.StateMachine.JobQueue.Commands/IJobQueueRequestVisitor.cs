using Raft.StateMachine.JobQueue.Commands.Dequeue;
using Raft.StateMachine.JobQueue.Commands.Enqueue;
using Raft.StateMachine.JobQueue.Commands.GetCount;

namespace Raft.StateMachine.JobQueue.Commands;

public interface IJobQueueRequestVisitor
{
    public void Visit(DequeueRequest request);
    public void Visit(EnqueueRequest request);
    public void Visit(GetCountRequest request);
}