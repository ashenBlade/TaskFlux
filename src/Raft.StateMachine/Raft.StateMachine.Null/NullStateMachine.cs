namespace Raft.StateMachine.Null;

public class NullStateMachine: IStateMachine
{
    public IResponse Apply(byte[] rawCommand)
    {
        return NullResponse.Instance;
    }
}