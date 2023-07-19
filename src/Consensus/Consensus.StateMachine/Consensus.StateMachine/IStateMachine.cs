namespace Consensus.StateMachine;

public interface IStateMachine<in TCommand, out TResponse>
{
    public TResponse Apply(TCommand request);
    public void ApplyNoResponse(TCommand rawCommand);
}

