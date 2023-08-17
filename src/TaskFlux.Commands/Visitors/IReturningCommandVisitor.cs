using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;

namespace TaskFlux.Commands;

public interface IReturningCommandVisitor<out T>
{
    public T Visit(EnqueueCommand command);
    public T Visit(DequeueCommand command);
    public T Visit(CountCommand command);
}