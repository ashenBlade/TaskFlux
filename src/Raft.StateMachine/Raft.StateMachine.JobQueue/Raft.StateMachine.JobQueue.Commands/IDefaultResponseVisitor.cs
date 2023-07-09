using Raft.StateMachine.JobQueue.Commands.Dequeue;
using Raft.StateMachine.JobQueue.Commands.Enqueue;
using Raft.StateMachine.JobQueue.Commands.Error;
using Raft.StateMachine.JobQueue.Commands.GetCount;

namespace Raft.StateMachine.JobQueue.Commands;

public interface IDefaultResponseVisitor
{
    public void Visit(DequeueResponse response);
    public void Visit(EnqueueResponse response);
    public void Visit(GetCountResponse response);
    public void Visit(ErrorResponse response);
}