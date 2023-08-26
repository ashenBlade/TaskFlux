using Consensus.CommandQueue;

namespace Consensus.Raft.Commands;

public abstract class Command<T, TCommand, TResponse> : ICommand<T>
{
    protected IConsensusModule<TCommand, TResponse> ConsensusModule { get; }

    protected Command(IConsensusModule<TCommand, TResponse> consensusModule)
    {
        ConsensusModule = consensusModule;
    }

    public abstract T Execute();
}

public abstract class Command<TCommand, TResponse> : ICommand
{
    protected IConsensusModule<TCommand, TResponse> ConsensusModule { get; }

    protected Command(IConsensusModule<TCommand, TResponse> consensusModule)
    {
        ConsensusModule = consensusModule;
    }

    public abstract void Execute();
}