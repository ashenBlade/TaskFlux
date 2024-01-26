using TaskFlux.Core.Commands.Count;
using TaskFlux.Core.Commands.CreateQueue;
using TaskFlux.Core.Commands.DeleteQueue;
using TaskFlux.Core.Commands.Dequeue;
using TaskFlux.Core.Commands.Enqueue;
using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Commands.ListQueues;
using TaskFlux.Core.Commands.Ok;
using TaskFlux.Core.Commands.PolicyViolation;

namespace TaskFlux.Core.Commands.Visitors;

public interface IResponseVisitor
{
    public void Visit(DequeueResponse response);
    public void Visit(EnqueueResponse response);
    public void Visit(CreateQueueResponse response);
    public void Visit(DeleteQueueResponse response);
    public void Visit(CountResponse response);
    public void Visit(ErrorResponse response);
    public void Visit(OkResponse response);
    public void Visit(ListQueuesResponse response);
    public void Visit(PolicyViolationResponse response);
}

public interface IResponseVisitor<out T>
{
    public T Visit(DequeueResponse response);
    public T Visit(EnqueueResponse response);
    public T Visit(CreateQueueResponse response);
    public T Visit(DeleteQueueResponse response);
    public T Visit(CountResponse response);
    public T Visit(ErrorResponse response);
    public T Visit(OkResponse response);
    public T Visit(ListQueuesResponse response);
    public T Visit(PolicyViolationResponse response);
}