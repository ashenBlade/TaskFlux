using Raft.CommandQueue;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Commands.Submit;
using Raft.Core.Log;
using Raft.StateMachine;

namespace Raft.Core.State;

internal abstract class BaseConsensusModuleState: IConsensusModuleState
{
    internal IConsensusModule ConsensusModule { get; }
    protected ILog Log => ConsensusModule.Log;
    protected Term CurrentTerm => ConsensusModule.CurrentTerm;
    protected NodeId? VotedFor => ConsensusModule.VotedFor;
    protected ICommandQueue CommandQueue => ConsensusModule.CommandQueue;
    protected IStateMachine StateMachine => ConsensusModule.StateMachine;
    protected NodeId Id => ConsensusModule.Id;
    protected ITimer ElectionTimer => ConsensusModule.ElectionTimer;
    protected ITimer HeartbeatTimer => ConsensusModule.HeartbeatTimer;
    protected IJobQueue JobQueue => ConsensusModule.JobQueue;
    protected PeerGroup PeerGroup => ConsensusModule.PeerGroup;

    protected IConsensusModuleState CurrentState
    {
        get => ConsensusModule.CurrentState;
        set => ConsensusModule.CurrentState = value;
    }

    internal BaseConsensusModuleState(IConsensusModule consensusModule)
    {
        ConsensusModule = consensusModule;
    }

    public abstract NodeRole Role { get; }
    public abstract RequestVoteResponse Apply(RequestVoteRequest request);
    public abstract AppendEntriesResponse Apply(AppendEntriesRequest request);
    public abstract SubmitResponse Apply(SubmitRequest request);
    public abstract void Dispose();
}