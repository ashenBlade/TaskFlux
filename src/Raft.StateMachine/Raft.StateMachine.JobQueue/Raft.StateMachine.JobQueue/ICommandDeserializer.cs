namespace Raft.StateMachine.JobQueue;

public interface ICommandDeserializer
{
    public ICommand Deserialize(byte[] payload);
}