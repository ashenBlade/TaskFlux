using Consensus.StateMachine.JobQueue.Commands.Batch;
using Consensus.StateMachine.JobQueue.Commands.Dequeue;
using Consensus.StateMachine.JobQueue.Commands.Enqueue;
using Consensus.StateMachine.JobQueue.Commands.GetCount;

namespace Consensus.StateMachine.JobQueue.Commands;

public interface IJobQueueRequestVisitor
{
    public void Visit(DequeueRequest request);
    public void Visit(EnqueueRequest request);
    public void Visit(GetCountRequest request);
    public void Visit(BatchRequest request);
}