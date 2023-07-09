namespace Raft.StateMachine.JobQueue.Commands.GetCount;

public class GetCountJobQueueResponse: JobQueueResponse
{
    private readonly GetCountResponse _response;

    public GetCountJobQueueResponse(GetCountResponse response): base((int)ResponseType.GetCount)
    {
        _response = response;
    }
    
    protected override void WriteTo(BinaryWriter writer)
    {
        writer.Write(_response.Count);    
    }
}