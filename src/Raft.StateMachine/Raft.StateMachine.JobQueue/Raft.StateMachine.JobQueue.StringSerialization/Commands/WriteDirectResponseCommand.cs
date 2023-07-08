using Raft.StateMachine.JobQueue.Requests;
using Raft.StateMachine.JobQueue.StringSerialization.Responses;

namespace Raft.StateMachine.JobQueue.StringSerialization.Commands;

public class WriteDirectResponseCommand: BaseStringResponse, ICommand
{
    private readonly string _response;

    public WriteDirectResponseCommand(string response)
    {
        _response = response;
    }
    public IResponse Apply(IJobQueueStateMachine stateMachine)
    {
        return this;
    }

    public void ApplyNoResponse(IJobQueueStateMachine stateMachine)
    { }

    protected override void Accept(StreamWriter writer)
    {
        writer.Write(_response);
    }
}