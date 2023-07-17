namespace Consensus.StateMachine;

public interface IStateMachine
{
    public IResponse Apply(byte[] rawCommand);
    public void ApplyNoResponse(byte[] rawCommand);
}

