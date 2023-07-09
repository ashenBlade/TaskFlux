namespace Raft.StateMachine.JobQueue.Commands.Error;

public record ErrorResponse(string Message): IDefaultResponse
{
    public static readonly ErrorResponse EmptyMessage = new(string.Empty);
    public ResponseType Type => ResponseType.Error;
    public void Accept(IDefaultResponseVisitor visitor)
    {
        visitor.Visit(this);
    }
}