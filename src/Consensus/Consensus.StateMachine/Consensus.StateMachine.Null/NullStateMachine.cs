namespace Consensus.StateMachine.Null;

public class NullStateMachine: IStateMachine
{
    public IResponse Apply(byte[] rawCommand)
    {
        return NullResponse.Instance;
    }

    public void ApplyNoResponse(byte[] rawCommand)
    { }
}