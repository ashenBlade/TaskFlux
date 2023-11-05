using TaskFlux.Abstractions;
using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.ListQueues;

public class ListQueuesResponse : Response
{
    public override ResponseType Type => ResponseType.ListQueues;
    public IReadOnlyCollection<ITaskQueueMetadata> Metadata { get; }

    public ListQueuesResponse(IReadOnlyCollection<ITaskQueueMetadata> metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        Metadata = metadata;
    }

    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncResponseVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}