namespace Consensus.StateMachine.JobQueue.Commands.Error;

public record ErrorResponse(string Message): IJobQueueResponse
{
    public static readonly ErrorResponse EmptyMessage = new(string.Empty);
    public ResponseType Type => ResponseType.Error;
    public void Accept(IJobQueueResponseVisitor visitor)
    {
        visitor.Visit(this);
    }
}