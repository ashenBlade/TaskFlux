using Raft.Core.Node;

namespace Raft.Core.Commands;

internal class MoveToCandidateAfterElectionTimerTimeoutCommand: UpdateCommand
{
    public MoveToCandidateAfterElectionTimerTimeoutCommand(INodeState previousState, INode node) 
        : base(previousState, node)
    { }

    protected override void ExecuteUpdate()
    {
        Node.ElectionTimer.Stop();
        Node.CurrentState = CandidateState.Create(Node);
        Node.UpdateState(Node.CurrentTerm.Increment(), Node.Id);
        Node.ElectionTimer.Start();
    }
}