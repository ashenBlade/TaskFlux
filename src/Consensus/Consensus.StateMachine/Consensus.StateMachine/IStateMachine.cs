namespace Consensus.StateMachine;

public interface IStateMachine<in TCommand, out TResponse>
{
    public TResponse Apply(TCommand command);
    public void ApplyNoResponse(TCommand command);
}

