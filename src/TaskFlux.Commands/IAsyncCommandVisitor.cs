using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;

namespace TaskFlux.Commands;

public interface IAsyncCommandVisitor
{
    public ValueTask VisitAsync(EnqueueCommand command, CancellationToken token = default);
    
    public ValueTask VisitAsync(DequeueCommand command, CancellationToken token = default);
    public ValueTask VisitAsync(CountCommand command, CancellationToken token = default);
}