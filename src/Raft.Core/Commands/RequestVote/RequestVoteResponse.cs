namespace Raft.Core.Commands;

public record RequestVoteResponse(Term CurrentTerm, bool VoteGranted)
{
    /// <summary>
    /// Term для обновления кандидата
    /// </summary>
    public Term CurrentTerm { get; set; } = CurrentTerm;

    /// <summary>
    /// true - узел принял запрос
    /// false - отверг 
    /// </summary>
    public bool VoteGranted { get; set; } = VoteGranted;
}