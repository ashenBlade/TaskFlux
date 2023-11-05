using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;

namespace TaskFlux.Commands.Visitors;

public interface IResponseVisitor
{
    public void Visit(EnqueueResponse response);
    public void Visit(DequeueResponse response);
    public void Visit(CountResponse response);
    public void Visit(ErrorResponse response);
    public void Visit(OkResponse response);
    public void Visit(ListQueuesResponse response);
}