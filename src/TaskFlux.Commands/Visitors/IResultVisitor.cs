using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;

namespace TaskFlux.Commands.Visitors;

public interface IResultVisitor
{
    public void Visit(EnqueueResult result);
    public void Visit(DequeueResult result);
    public void Visit(CountResult result);
    public void Visit(ErrorResult result);
    public void Visit(OkResult result);
    public void Visit(ListQueuesResult result);
}