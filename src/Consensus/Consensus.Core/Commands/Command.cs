using Consensus.CommandQueue;

namespace Consensus.Core.Commands;

public abstract class Command<T>: ICommand<T>
{
    protected IConsensusModule ConsensusModule { get; }
    protected Command(IConsensusModule consensusModule)
    {
        ConsensusModule = consensusModule;
    }
    public abstract T Execute();
}

public abstract class Command : ICommand
{
    protected IConsensusModule ConsensusModule { get; }
    protected Command(IConsensusModule consensusModule)
    {
        ConsensusModule = consensusModule;
    }
    public abstract void Execute();
}
