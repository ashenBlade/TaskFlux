using Raft.CommandQueue;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Commands.Submit;
using Raft.Core.Log;

namespace Raft.Core.Node;

internal abstract class BaseNodeState: INodeState
{
    internal INode Node { get; }
    protected ILog Log => Node.Log;

    protected Term CurrentTerm
    {
        get => Node.CurrentTerm;
        set => Node.CurrentTerm = value;
    }
    
    protected NodeId? VotedFor
    {
        get => Node.VotedFor;
        set => Node.VotedFor = value;
    }
    protected ICommandQueue CommandQueue => Node.CommandQueue;
    protected IStateMachine StateMachine => Node.StateMachine;
    protected NodeId Id => Node.Id;
    protected ITimer ElectionTimer => Node.ElectionTimer;
    protected ITimer HeartbeatTimer => Node.HeartbeatTimer;
    protected IJobQueue JobQueue => Node.JobQueue;
    protected PeerGroup PeerGroup => Node.PeerGroup;

    protected INodeState CurrentState
    {
        get => Node.CurrentState;
        set => Node.CurrentState = value;
    }

    internal BaseNodeState(INode node)
    {
        Node = node;
    }

    public abstract NodeRole Role { get; }
    public abstract RequestVoteResponse Apply(RequestVoteRequest request);
    public abstract AppendEntriesResponse Apply(AppendEntriesRequest request);
    public abstract SubmitResponse Apply(SubmitRequest request);
    public abstract void Dispose();
}