using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;

namespace TaskFlux.Commands;

public interface IResultVisitor
{
    public void Visit(EnqueueResult result);
    public void Visit(DequeueResult result);
    public void Visit(CountResult result);
}