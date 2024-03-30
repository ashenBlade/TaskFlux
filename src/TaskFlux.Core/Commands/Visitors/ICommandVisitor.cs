using TaskFlux.Core.Commands.Count;
using TaskFlux.Core.Commands.CreateQueue;
using TaskFlux.Core.Commands.DeleteQueue;
using TaskFlux.Core.Commands.Dequeue;
using TaskFlux.Core.Commands.Enqueue;
using TaskFlux.Core.Commands.ListQueues;

namespace TaskFlux.Core.Commands.Visitors;

public interface ICommandVisitor
{
    public void Visit(EnqueueCommand command);
    public void Visit(ImmediateDequeueCommand command);
    public void Visit(CountCommand command);
    public void Visit(CreateQueueCommand command);
    public void Visit(DeleteQueueCommand command);
    public void Visit(ListQueuesCommand command);
    public void Visit(ReturnRecordCommand command);
    public void Visit(CommitDequeueCommand command);
    public void Visit(AwaitableDequeueCommand command);
}

public interface ICommandVisitor<out T>
{
    public T Visit(EnqueueCommand command);
    public T Visit(ImmediateDequeueCommand command);
    public T Visit(CountCommand command);
    public T Visit(CreateQueueCommand command);
    public T Visit(DeleteQueueCommand command);
    public T Visit(ListQueuesCommand command);
    public T Visit(ReturnRecordCommand command);
    public T Visit(CommitDequeueCommand command);
    public T Visit(AwaitableDequeueCommand command);
}