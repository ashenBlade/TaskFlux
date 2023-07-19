namespace Consensus.StateMachine.Null;

public class NullStateMachine<T, R>: IStateMachine<T, R>
{
    public R Apply(T request)
    {
        return default!;
    }

    public void ApplyNoResponse(T rawCommand)
    { }
}