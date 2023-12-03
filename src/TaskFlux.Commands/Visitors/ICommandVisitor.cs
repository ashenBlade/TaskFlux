using TaskFlux.Commands.Count;
using TaskFlux.Commands.CreateQueue;
using TaskFlux.Commands.DeleteQueue;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.ListQueues;

namespace TaskFlux.Commands.Visitors;

public interface ICommandVisitor
{
    public void Visit(EnqueueCommand command);
    public void Visit(DequeueCommand command);
    public void Visit(CountCommand command);
    public void Visit(CreateQueueCommand command);
    public void Visit(DeleteQueueCommand command);
    public void Visit(ListQueuesCommand command);
}

public interface ICommandVisitor<out T>
{
    public T Visit(EnqueueCommand command);
    public T Visit(DequeueCommand command);
    public T Visit(CountCommand command);
    public T Visit(CreateQueueCommand command);
    public T Visit(DeleteQueueCommand command);
    public T Visit(ListQueuesCommand command);
}