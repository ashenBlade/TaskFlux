using Raft.Core.State;
using Serilog;

namespace Raft.Core.Commands;

internal abstract class UpdateCommand : Command
{
    private readonly IConsensusModuleState _previousState;

    protected UpdateCommand(IConsensusModuleState previousState, IConsensusModule consensusModule)
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