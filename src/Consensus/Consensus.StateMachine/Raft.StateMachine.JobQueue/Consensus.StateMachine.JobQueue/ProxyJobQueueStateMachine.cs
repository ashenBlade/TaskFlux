using JobQueue.Core;

namespace Consensus.StateMachine.JobQueue;

public class ProxyJobQueueStateMachine: IStateMachine
{
    private readonly IJobQueue _jobQueue;
    private readonly ICommandDeserializer _deserializer;

    public ProxyJobQueueStateMachine(IJobQueue jobQueue,
                                     ICommandDeserializer deserializer)
    {
        _jobQueue = jobQueue;
        _deserializer = deserializer;
    }
    
    public IResponse Apply(byte[] rawCommand)
    {
        var command = _deserializer.Deserialize(rawCommand);
        var response = command.Apply(_jobQueue);
        return new JobQueueResponse(response);
    }

    public void ApplyNoResponse(byte[] rawCommand)
    {
        var command = _deserializer.Deserialize(rawCommand);
        command.ApplyNoResponse(_jobQueue);
    }
}