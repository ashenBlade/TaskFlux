using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;

namespace TaskFlux.Commands.Visitors;

public interface IAsyncResponseVisitor
{
    public ValueTask VisitAsync(EnqueueResponse response, CancellationToken token = default);
    public ValueTask VisitAsync(DequeueResponse response, CancellationToken token = default);
    public ValueTask VisitAsync(CountResponse response, CancellationToken token = default);
    public ValueTask VisitAsync(ErrorResponse response, CancellationToken token);
    public ValueTask VisitAsync(OkResponse response, CancellationToken token);
    public ValueTask VisitAsync(ListQueuesResponse response, CancellationToken token);
}