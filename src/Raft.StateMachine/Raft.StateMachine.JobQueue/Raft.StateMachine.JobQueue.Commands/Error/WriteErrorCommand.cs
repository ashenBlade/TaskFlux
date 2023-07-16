using JobQueue.Core;

namespace Raft.StateMachine.JobQueue.Commands.Error;

public class WriteErrorCommand: IJobQueueCommand
{
    private readonly ErrorResponse _response;

    public WriteErrorCommand(ErrorResponse response)
    {
        _response = response;
    }

    public IJobQueueResponse Apply(IJobQueue jobQueue)
    {
        return _response;
    }

    public void ApplyNoResponse(IJobQueue jobQueue)
    { }

    public static readonly WriteErrorCommand UnknownError =
        new(new ErrorResponse("Неизвестная ошибка"));
}