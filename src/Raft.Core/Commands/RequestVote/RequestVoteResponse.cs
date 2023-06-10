namespace Raft.Core.Commands;

public class RequestVoteResponse
{
    /// <summary>
    /// Term для обновления кандидата
    /// </summary>
    public Term CurrentTerm { get; set; }
    /// <summary>
    /// true - узел принял запрос
    /// false - отверг 
    /// </summary>
    public bool VoteGranted { get; set; }
}