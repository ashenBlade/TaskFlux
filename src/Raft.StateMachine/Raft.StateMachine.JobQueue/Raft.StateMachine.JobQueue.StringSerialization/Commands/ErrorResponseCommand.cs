using Raft.StateMachine.JobQueue.Requests;
using Raft.StateMachine.JobQueue.StringSerialization.Responses;

namespace Raft.StateMachine.JobQueue.StringSerialization.Commands;

public class ErrorResponseCommand: BaseStringResponse, ICommand
{
    private readonly string _errorMessage;

    public ErrorResponseCommand(string errorMessage)
    {
        _errorMessage = errorMessage;
    }

    protected override void Accept(StreamWriter writer)
    {
        writer.Write("error: ");
        writer.Write(_errorMessage);
    }

    public IResponse Apply(IJobQueueStateMachine stateMachine)
    {
        return this;
    }
}