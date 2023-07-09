namespace Raft.StateMachine.JobQueue.Commands.Error;

public class ErrorJobQueueResponse: JobQueueResponse
{
    private readonly ErrorResponse _response;

    public ErrorJobQueueResponse(ErrorResponse response): base((int)ResponseType.Error)
    {
        _response = response;
    }
    
    protected override void WriteTo(BinaryWriter writer)
    {
        writer.Write(_response.Message);
    }
}