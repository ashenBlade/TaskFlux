using JobQueue.Core;

namespace Consensus.StateMachine.JobQueue.Commands;

public class SerializerJobQueueCommand: ICommand
{
    private readonly IJobQueueCommand _command;
    private readonly IJobQueueResponseSerializer _serializer;

    public SerializerJobQueueCommand(IJobQueueCommand command, IJobQueueResponseSerializer serializer)
    {
        _command = command;
        _serializer = serializer;
    }
    public ICommandResponse Apply(IJobQueue jobQueue)
    {
        return new SerializerJobQueueCommandResponse(_command.Apply(jobQueue), _serializer);
    }

    public void ApplyNoResponse(IJobQueue jobQueue)
    {
        _command.ApplyNoResponse(jobQueue);
    }
}