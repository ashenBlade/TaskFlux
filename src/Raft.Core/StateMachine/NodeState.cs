using System.ComponentModel;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Log;

namespace Raft.Core.StateMachine;

internal abstract class NodeState: INodeState
{
    internal readonly IStateMachine StateMachine;
    protected INode Node => StateMachine.Node;
    protected ILog Log => StateMachine.Log;
    private volatile bool _stopped = false;

    internal NodeState(IStateMachine stateMachine)
    {
        StateMachine = stateMachine;
    }

    public abstract NodeRole Role { get; }
    public virtual RequestVoteResponse Apply(RequestVoteRequest request)
    {
        if (_stopped)
        {
            throw new InvalidOperationException("Невозможно применить команду - состояние уже изменилось");
        }
        
        // Мы в более актуальном Term'е
        if (request.CandidateTerm < Node.CurrentTerm)
        {
            return new RequestVoteResponse(CurrentTerm: Node.CurrentTerm, VoteGranted: false);
        }

        var canVote = 
            // Ранее не голосовали
            Node.VotedFor is null || 
            // Текущий лидер/кандидат посылает этот запрос (почему бы не согласиться)
            Node.VotedFor == request.CandidateId;
        
        // Отдать свободный голос можем только за кандидата 
        if (canVote && 
            // С термом больше нашего (иначе, на текущем терме уже есть лидер)
            Node.CurrentTerm < request.CandidateTerm && 
            // У которого лог не "младше" нашего
            StateMachine.Log.LastLogEntry.IsUpToDateWith(request.LastLog))
        {
            var command = new MoveToFollowerStateCommand(request.CandidateTerm, request.CandidateId, this, StateMachine);
            StateMachine.CommandQueue.Enqueue(command);
            
            // И подтвердим свой 
            return new RequestVoteResponse(CurrentTerm: Node.CurrentTerm, VoteGranted: true);
        }
        
        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse(CurrentTerm: Node.CurrentTerm, VoteGranted: false);
    }

    public virtual HeartbeatResponse Apply(HeartbeatRequest request)
    {
        if (_stopped)
        {
            throw new InvalidOperationException("Невозможно применить команду - состояние уже изменилось");
        }
        
        if (request.Term <= Node.CurrentTerm)
        {
            return HeartbeatResponse.Fail(Node.CurrentTerm);
        }

        LogEntryCheckResult checkResult;
        switch (checkResult = Log.Check(request.PrevLogEntry))
        {
            case LogEntryCheckResult.Conflict:
                return HeartbeatResponse.Fail(Node.CurrentTerm);
            case LogEntryCheckResult.Contains:
            case LogEntryCheckResult.NotFound:
                break;
            default:
                throw new InvalidEnumArgumentException(nameof(LogEntryCheckResult), (int)checkResult, typeof(LogEntryCheckResult));
        }

        
        if (Log.CommitIndex < request.LeaderCommit)
        {
            Log.CommitIndex = Math.Min(request.LeaderCommit, Log.LastLogEntry.Index);
        }

        if (Node.CurrentTerm < request.Term)
        {
            StateMachine.CommandQueue.Enqueue(new MoveToFollowerStateCommand(request.Term, null, this, StateMachine));
        }

        return HeartbeatResponse.Ok(Node.CurrentTerm);
    }

    public virtual void Dispose()
    {
        _stopped = true;
    }
}