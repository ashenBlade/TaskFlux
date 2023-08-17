using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;

namespace TaskFlux.Commands;

public interface IResultVisitor
{
    public void Visit(EnqueueResult result);
    public void Visit(DequeueResult result);
    public void Visit(CountResult result);
    public void Visit(ErrorResult result);
}