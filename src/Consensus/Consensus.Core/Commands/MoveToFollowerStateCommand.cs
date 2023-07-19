using Consensus.Core.State;

namespace Consensus.Core.Commands;

internal class MoveToFollowerStateCommand<TCommand, TResponse>: UpdateCommand<TCommand, TResponse>
{
    private readonly Term _term;
    private readonly NodeId? _votedFor;

    public MoveToFollowerStateCommand(Term term, NodeId? votedFor, IConsensusModuleState<TCommand, TResponse> previousState, IConsensusModule<TCommand, TResponse> consensusModule) 
        : base(previousState, consensusModule)
    {
        _term = term;
        _votedFor = votedFor;
    }

    protected override void ExecuteUpdate()
    {
        ConsensusModule.CurrentState = FollowerState.Create(ConsensusModule);
        ConsensusModule.ElectionTimer.Start();
        ConsensusModule.UpdateState(_term, _votedFor);
    }
}