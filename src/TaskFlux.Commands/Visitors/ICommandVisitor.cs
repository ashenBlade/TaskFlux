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