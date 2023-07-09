namespace Raft.StateMachine.JobQueue.Commands.Enqueue;

public class EnqueueJobQueueResponse: JobQueueResponse
{
    private readonly EnqueueResponse _response;

    public EnqueueJobQueueResponse(EnqueueResponse response): base((int)ResponseType.Enqueue)
    {
        _response = response;
    }
    
    protected override void WriteTo(BinaryWriter writer)
    {
        writer.Write(_response.Success);
    }
}