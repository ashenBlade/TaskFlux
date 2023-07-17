using Consensus.StateMachine.JobQueue.Commands.Batch;
using Consensus.StateMachine.JobQueue.Commands.Dequeue;
using Consensus.StateMachine.JobQueue.Commands.Enqueue;
using Consensus.StateMachine.JobQueue.Commands.Error;
using Consensus.StateMachine.JobQueue.Commands.GetCount;

namespace Consensus.StateMachine.JobQueue.Commands;

public interface IJobQueueResponseVisitor
{
    public void Visit(DequeueResponse response);
    public void Visit(EnqueueResponse response);
    public void Visit(GetCountResponse response);
    public void Visit(ErrorResponse response);
    public void Visit(BatchResponse response);
}