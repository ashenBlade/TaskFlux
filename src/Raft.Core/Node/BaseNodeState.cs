using System.ComponentModel;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Log;

namespace Raft.Core.Node;

internal abstract class BaseNodeState: INodeState
{
    internal readonly INode Node;
    protected ILog Log => Node.Log;

    internal BaseNodeState(INode node)
    {
        Node = node;
    }

    public abstract NodeRole Role { get; }
    public virtual RequestVoteResponse Apply(RequestVoteRequest request)
    {
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
            Node.Log.LastLogEntry.IsUpToDateWith(request.LastLog))
        {
            Node.CurrentState = FollowerState.Create(Node);
            Node.ElectionTimer.Start();
            Node.CurrentTerm = request.CandidateTerm;
            Node.VotedFor = request.CandidateId;
            
            // И подтвердим свой 
            return new RequestVoteResponse(CurrentTerm: Node.CurrentTerm, VoteGranted: true);
        }
        
        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse(CurrentTerm: Node.CurrentTerm, VoteGranted: false);
    }

    public virtual HeartbeatResponse Apply(HeartbeatRequest request)
    {
        if (request.Term < Node.CurrentTerm)
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
            Node.CurrentState = FollowerState.Create(Node);
            Node.ElectionTimer.Start();
            Node.CurrentTerm = request.Term;
            Node.VotedFor = null;
        }

        return HeartbeatResponse.Ok(Node.CurrentTerm);
    }

    public virtual void Dispose()
    { }
}