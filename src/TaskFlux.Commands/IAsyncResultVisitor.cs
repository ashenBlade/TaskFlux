using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;

namespace TaskFlux.Commands;

public interface IAsyncResultVisitor
{
    public ValueTask VisitAsync(EnqueueResult result, CancellationToken token = default);
    public ValueTask VisitAsync(DequeueResult result, CancellationToken token = default);
    public ValueTask VisitAsync(CountResult result, CancellationToken token = default);
}