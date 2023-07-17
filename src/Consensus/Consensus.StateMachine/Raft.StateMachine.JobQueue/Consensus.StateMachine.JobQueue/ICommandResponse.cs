namespace Consensus.StateMachine.JobQueue;

public interface ICommandResponse
{
    public void WriteTo(BinaryWriter writer);
}