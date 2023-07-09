using JobQueue.Core;

namespace Raft.StateMachine.JobQueue.Commands.Error;

public class WriteErrorCommand: ICommand
{
    private readonly ErrorResponse _response = ErrorResponse.EmptyMessage;
    private readonly ErrorJobQueueResponse? _cachedResponse;

    public WriteErrorCommand(ErrorResponse response)
    {
        _response = response;
    }

    private WriteErrorCommand(ErrorJobQueueResponse cached)
    {
        _cachedResponse = cached;
    }
    
    public JobQueueResponse Apply(IJobQueue jobQueue)
    {
        return _cachedResponse ?? new ErrorJobQueueResponse(_response);
    }

    public void ApplyNoResponse(IJobQueue jobQueue)
    { }

    public static readonly WriteErrorCommand UnknownError =
        new(new ErrorJobQueueResponse(new ErrorResponse("Неизвестная ошибка")));
}