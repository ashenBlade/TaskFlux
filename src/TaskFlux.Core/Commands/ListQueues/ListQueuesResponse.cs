using TaskFlux.Core.Commands.Visitors;
using TaskFlux.Core.Queue;

namespace TaskFlux.Core.Commands.ListQueues;

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

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}