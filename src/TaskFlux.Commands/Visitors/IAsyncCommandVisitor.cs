using TaskFlux.Commands.Count;
using TaskFlux.Commands.CreateQueue;
using TaskFlux.Commands.DeleteQueue;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.ListQueues;

namespace TaskFlux.Commands.Visitors;

public interface IAsyncCommandVisitor
{
    public ValueTask VisitAsync(EnqueueCommand command, CancellationToken token = default);
    
    public ValueTask VisitAsync(DequeueCommand command, CancellationToken token = default);
    public ValueTask VisitAsync(CountCommand command, CancellationToken token = default);
    public ValueTask VisitAsync(CreateQueueCommand command, CancellationToken token = default);
    public ValueTask VisitAsync(DeleteQueueCommand command, CancellationToken token = default);
    public ValueTask VisitAsync(ListQueuesCommand command, CancellationToken token = default);
}