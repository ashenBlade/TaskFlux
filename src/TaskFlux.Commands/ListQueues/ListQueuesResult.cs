using TaskFlux.Commands.Visitors;
using TaskQueue.Core;

namespace TaskFlux.Commands.ListQueues;

public class ListQueuesResult : Result
{
    public override ResultType Type => ResultType.ListQueues;
    public IReadOnlyCollection<ITaskQueueMetadata> Metadata { get; }

    public ListQueuesResult(IReadOnlyCollection<ITaskQueueMetadata> metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        Metadata = metadata;
    }

    public override void Accept(IResultVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncResultVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}