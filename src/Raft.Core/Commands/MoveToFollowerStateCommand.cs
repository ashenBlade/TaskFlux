using Raft.Core.State;

namespace Raft.Core.Commands;

internal class MoveToFollowerStateCommand: UpdateCommand
{
    private readonly Term _term;
    private readonly NodeId? _votedFor;

    public MoveToFollowerStateCommand(Term term, NodeId? votedFor, IConsensusModuleState previousState, IConsensusModule consensusModule) 
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