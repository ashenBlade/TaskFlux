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

public interface IAsyncResponseVisitor
{
    public ValueTask VisitAsync(DequeueResponse response, CancellationToken token);
    public ValueTask VisitAsync(EnqueueResponse response, CancellationToken token);
    public ValueTask VisitAsync(CreateQueueResponse response, CancellationToken token);
    public ValueTask VisitAsync(DeleteQueueResponse response, CancellationToken token);
    public ValueTask VisitAsync(CountResponse response, CancellationToken token);
    public ValueTask VisitAsync(ErrorResponse response, CancellationToken token);
    public ValueTask VisitAsync(OkResponse response, CancellationToken token);
    public ValueTask VisitAsync(ListQueuesResponse response, CancellationToken token);
    public ValueTask VisitAsync(PolicyViolationResponse response, CancellationToken token);
}