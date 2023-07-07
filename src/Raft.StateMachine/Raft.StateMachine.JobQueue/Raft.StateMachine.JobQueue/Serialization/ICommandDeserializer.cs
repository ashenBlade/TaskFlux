using Raft.StateMachine.JobQueue.Requests;

namespace Raft.StateMachine.JobQueue.Serialization;

public interface ICommandDeserializer
{
    public ICommand Deserialize(byte[] payload);
}