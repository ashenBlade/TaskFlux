using System.Collections.Concurrent;
using Raft.Core.Node;

namespace Raft.Core.Commands;

public class MoveToFollowerStateCommand: UpdateCommand
{
    private readonly Term _term;
    private readonly NodeId? _votedFor;

    public MoveToFollowerStateCommand(Term term, NodeId? votedFor, INodeState previousState, INode node) 
        : base(previousState, node)
    {
        _term = term;
        _votedFor = votedFor;
    }

    protected override void ExecuteUpdate()
    {
        Node.CurrentState = FollowerState.Create(Node);
        Node.ElectionTimer.Start();
        Node.CurrentTerm = _term;
        Node.VotedFor = _votedFor;
    }
}