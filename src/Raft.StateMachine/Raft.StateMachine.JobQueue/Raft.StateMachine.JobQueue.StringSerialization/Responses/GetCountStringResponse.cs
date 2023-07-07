using Raft.StateMachine.JobQueue.Requests;

namespace Raft.StateMachine.JobQueue.StringSerialization.Responses;

public class GetCountStringResponse: BaseStringResponse
{
    private readonly GetCountResponse _response;

    public GetCountStringResponse(GetCountResponse response)
    {
        _response = response;
    }
    
    protected override void Accept(StreamWriter writer)
    {
        writer.Write(_response.Count);
    }
}