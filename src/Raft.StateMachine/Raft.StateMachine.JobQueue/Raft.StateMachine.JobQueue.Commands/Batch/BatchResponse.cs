namespace Raft.StateMachine.JobQueue.Commands.Batch;

public class BatchResponse: IJobQueueResponse
{
    public BatchResponse(ICollection<IJobQueueResponse> responses)
    {
        Responses = responses;
    }

    public ResponseType Type => ResponseType.Batch;
    public ICollection<IJobQueueResponse> Responses { get; }
    public void Accept(IJobQueueResponseVisitor visitor)
    {
        visitor.Visit(this);
    }
}