using Consensus.Core.State;

namespace Consensus.Core.Commands;

internal abstract class UpdateCommand<TCommand, TResponse> : Command<TCommand, TResponse>
{
    private readonly ConsensusModuleState<TCommand, TResponse> _previousState;

    protected UpdateCommand(ConsensusModuleState<TCommand, TResponse> previousState, IConsensusModule<TCommand, TResponse> consensusModule)
    :base(consensusModule)
    {
        _previousState = previousState;
    }
    
    public override void Execute()
    {
        if (ConsensusModule.CurrentState == _previousState)
        {
            ExecuteUpdate();
        }
    }

    protected abstract void ExecuteUpdate();
}