using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;

namespace TaskFlux.Commands;

public interface ICommandVisitor
{
    public void Visit(EnqueueCommand command);
    public void Visit(DequeueCommand command);
    public void Visit(CountCommand command);
}