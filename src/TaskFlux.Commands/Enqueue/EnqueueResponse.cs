using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.Enqueue;

public class EnqueueResponse : Response
{
    public static readonly EnqueueResponse Ok = new(true);
    public static readonly EnqueueResponse Full = new(false);
    public override ResponseType Type => ResponseType.Enqueue;
    public bool Success { get; }

    public EnqueueResponse(bool success)
    {
        Success = success;
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