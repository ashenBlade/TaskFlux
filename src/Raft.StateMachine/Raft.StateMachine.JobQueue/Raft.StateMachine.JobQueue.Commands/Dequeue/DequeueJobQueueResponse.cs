namespace Raft.StateMachine.JobQueue.Commands.Dequeue;

public class DequeueJobQueueResponse: JobQueueResponse
{
    private readonly DequeueResponse _response;

    public DequeueJobQueueResponse(DequeueResponse response): base((int)ResponseType.Dequeue)
    {
        _response = response;
    }

    protected override void WriteTo(BinaryWriter writer)
    {
        writer.Write(_response.Success);
        writer.Write(_response.Key);
        writer.Write(_response.Payload.Length);
        writer.Write(_response.Payload);
    }
}