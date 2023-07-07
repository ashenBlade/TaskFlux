using Raft.StateMachine.JobQueue.Requests;

namespace Raft.StateMachine.JobQueue.StringSerialization.Responses;

public class EnqueueStringResponse: BaseStringResponse
{
    private readonly EnqueueResponse _response;

    public EnqueueStringResponse(EnqueueResponse response)
    {
        _response = response;
    }
    
    protected override void Accept(StreamWriter writer)
    {
        if (_response.Success)
        {
            writer.Write("ok");
        }
        else
        {
            writer.Write("fail");
        }
    }
}