namespace Raft.StateMachine;

public interface IStateMachine
{
    public IResponse Apply(byte[] rawCommand);
}

