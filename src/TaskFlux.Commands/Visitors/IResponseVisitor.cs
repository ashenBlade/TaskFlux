using TaskFlux.Commands.Count;
using TaskFlux.Commands.CreateQueue;
using TaskFlux.Commands.DeleteQueue;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.PolicyViolation;

namespace TaskFlux.Commands.Visitors;

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