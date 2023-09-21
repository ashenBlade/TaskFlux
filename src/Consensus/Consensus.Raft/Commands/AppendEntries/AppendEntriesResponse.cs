namespace Consensus.Raft.Commands.AppendEntries;

public record AppendEntriesResponse(Term Term, bool Success)
{
    public static AppendEntriesResponse Ok(Term term) => new(term, true);
    public static AppendEntriesResponse Fail(Term term) => new(term, false);
}