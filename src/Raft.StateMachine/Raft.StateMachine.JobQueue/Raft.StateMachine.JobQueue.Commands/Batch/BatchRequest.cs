namespace Raft.StateMachine.JobQueue.Commands.Batch;

public class BatchRequest: IJobQueueRequest
{
    public RequestType Type => RequestType.BatchRequest;
    public ICollection<IJobQueueRequest> Requests { get; }
    public BatchRequest(ICollection<IJobQueueRequest> requests)
    {
        Requests = requests;
    }

    public void Accept(IJobQueueRequestVisitor visitor)
    {
        visitor.Visit(this);
    }
}