using System.ComponentModel;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Commands.Submit;
using Raft.Core.Log;
using Serilog;

namespace Raft.Core.Node;

internal abstract class BaseNodeState: INodeState
{
    internal readonly INode Node;
    private readonly ILogger _logger;
    protected ILog Log => Node.Log;

    internal BaseNodeState(INode node, ILogger logger)
    {
        Node = node;
        _logger = logger;
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
            // У которого лог в консистентном с нашим состоянием
            Node.Log.IsConsistentWith(request.LastLogEntryInfo))
        {
            Node.CurrentState = FollowerState.Create(Node);
            Node.ElectionTimer.Start();
            Node.CurrentTerm = request.CandidateTerm;
            Node.VotedFor = request.CandidateId;
            
            return new RequestVoteResponse(CurrentTerm: Node.CurrentTerm, VoteGranted: true);
        }
        
        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse(CurrentTerm: Node.CurrentTerm, VoteGranted: false);
    }


    public virtual AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        if (request.Term < Node.CurrentTerm)
        {
            return AppendEntriesResponse.Fail(Node.CurrentTerm);
        }

        if (Log.IsConsistentWith(request.PrevLogEntryInfo) is false)
        {
            return AppendEntriesResponse.Fail(Node.CurrentTerm);
        }
        
        if (request.Entries.Count > 0)
        {
            // Записи могут перекрываться. (например, новый лидер затирает старые записи)
            // Поэтому необходимо найти индекс,
            // начиная с которого необходимо добавить в лог новые записи.

            // Индекс расхождения в нашем логе
            var logIndex = request.PrevLogEntryInfo.Index + 1;
            
            // Индекс в массиве вхождений
            var newEntriesIndex = 0;
        
            for (; 
                 logIndex < Log.Entries.Count && 
                 newEntriesIndex < request.Entries.Count; 
                 logIndex++, newEntriesIndex++)
            {
                if (Log.Entries[logIndex].Term != request.Entries[newEntriesIndex].Term)
                {
                    break;
                }
            }

            // Может случиться так, что все присланные вхождения уже есть в нашем логе
            if (newEntriesIndex < request.Entries.Count)
            {
                Log.AppendUpdateRange(request.Entries.Skip(newEntriesIndex), logIndex);
            }
        }

        if (Log.CommitIndex < request.LeaderCommit)
        {
            Log.Commit(Math.Min(request.LeaderCommit, Log.LastEntry.Index));
        }

        if (Node.CurrentTerm < request.Term)
        {
            Node.CurrentState = FollowerState.Create(Node);
            Node.ElectionTimer.Start();
            Node.CurrentTerm = request.Term;
            Node.VotedFor = null;
        }

        return AppendEntriesResponse.Ok(Node.CurrentTerm);
    }

    public virtual SubmitResponse Apply(SubmitRequest request)
    {
        throw new InvalidOperationException("Пошел нахуй");
    }

    public virtual void Dispose()
    { }
}