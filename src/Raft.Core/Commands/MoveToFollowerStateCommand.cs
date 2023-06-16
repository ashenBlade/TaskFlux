using System.Collections.Concurrent;
using Raft.Core.StateMachine;

namespace Raft.Core.Commands;

public class MoveToFollowerStateCommand: UpdateCommand
{
    private readonly Term _term;
    private readonly PeerId? _votedFor;

    public MoveToFollowerStateCommand(Term term, PeerId? votedFor, INodeState previousState, IStateMachine stateMachine) 
        : base(previousState, stateMachine)
    {
        _term = term;
        _votedFor = votedFor;
    }

    protected override void ExecuteUpdate()
    {
        StateMachine.CurrentState = FollowerState.Create(StateMachine);
        StateMachine.ElectionTimer.Start();
        StateMachine.Node.CurrentTerm = _term;
        StateMachine.Node.VotedFor = _votedFor;
    }
}