using System.Text;
using Raft.StateMachine.JobQueue.Requests;

namespace Raft.StateMachine.JobQueue.StringSerialization.Responses;

public class DequeueStringResponse: BaseStringResponse
{
    private readonly DequeueResponse _response;

    public DequeueStringResponse(DequeueResponse response)
    {
        _response = response;
    }

    protected override void Accept(StreamWriter writer)
    {
        if (_response.Success)
        {
            writer.Write("ok ");
            writer.Write(_response.Key);
            writer.Write(" ");
            var payload = Convert.ToBase64String(_response.Payload);
            writer.Write(payload);
        }
        else
        {
            writer.Write("empty");
        }
    }
}