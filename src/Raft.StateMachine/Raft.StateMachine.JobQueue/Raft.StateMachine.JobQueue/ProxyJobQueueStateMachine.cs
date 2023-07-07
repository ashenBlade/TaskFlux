using Raft.StateMachine.JobQueue.Serialization;

namespace Raft.StateMachine.JobQueue;

public class ProxyJobQueueStateMachine: IStateMachine
{
    private readonly IJobQueueStateMachine _stateMachine;
    private readonly ICommandDeserializer _deserializer;

    public ProxyJobQueueStateMachine(IJobQueueStateMachine stateMachine, 
                                     ICommandDeserializer deserializer)
    {
        _stateMachine = stateMachine;
        _deserializer = deserializer;
    }
    public IResponse Apply(byte[] rawCommand)
    {
        var command = _deserializer.Deserialize(rawCommand);
        return command.Apply(_stateMachine);
    }
}