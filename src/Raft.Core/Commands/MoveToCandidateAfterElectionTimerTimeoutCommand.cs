using System.Windows.Input;
using Raft.Core.Node;

namespace Raft.Core.Commands;

public class MoveToCandidateAfterElectionTimerTimeoutCommand: UpdateCommand
{
    public MoveToCandidateAfterElectionTimerTimeoutCommand(INodeState previousState, INode node) 
        : base(previousState, node)
    { }

    protected override void ExecuteUpdate()
    {
        Node.ElectionTimer.Stop();
        Node.CurrentState = CandidateState.Create(Node);
        Node.CurrentTerm = Node.CurrentTerm.Increment();
        Node.VotedFor = Node.Id;
        Node.ElectionTimer.Start();
    }
}