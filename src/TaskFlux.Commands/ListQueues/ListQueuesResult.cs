using JobQueue.Core;
using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.ListQueues;

public class ListQueuesResult: Result
{
    public override ResultType Type => ResultType.ListQueues;
    public IReadOnlyCollection<IJobQueueMetadata> Metadata { get; }
    public ListQueuesResult(IReadOnlyCollection<IJobQueueMetadata> metadata)
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