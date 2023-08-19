using TaskFlux.Commands.Count;
using TaskFlux.Commands.CreateQueue;
using TaskFlux.Commands.DeleteQueue;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.ListQueues;

namespace TaskFlux.Commands.Visitors;

public interface IReturningCommandVisitor<out T>
{
    public T Visit(EnqueueCommand command);
    public T Visit(DequeueCommand command);
    public T Visit(CountCommand command);
    public T Visit(CreateQueueCommand command);
    public T Visit(DeleteQueueCommand command);
    public T Visit(ListQueuesCommand command);
}