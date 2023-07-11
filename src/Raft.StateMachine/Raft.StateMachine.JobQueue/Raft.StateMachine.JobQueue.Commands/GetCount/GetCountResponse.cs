namespace Raft.StateMachine.JobQueue.Commands.GetCount;

public record GetCountResponse(int Count): IJobQueueResponse
{
    public static readonly GetCountResponse Empty = new(0);
    public ResponseType Type => ResponseType.GetCount;
    public void Accept(IJobQueueResponseVisitor visitor)
    {
        visitor.Visit(this);
    }
}
