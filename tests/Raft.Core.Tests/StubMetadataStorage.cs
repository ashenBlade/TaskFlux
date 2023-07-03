namespace Raft.Core.Tests;

public class StubMetadataStorage: IMetadataStorage
{
    public StubMetadataStorage(Term term, NodeId? votedFor)
    {
        Term = term;
        VotedFor = votedFor;
    }

    private Term _term;

    public Term Term
    {
        set => _term = value;
    }

    public Term ReadTerm() => _term;

    private NodeId? _votedFor;

    public NodeId? VotedFor
    {
        set => _votedFor = value;
    }

    public NodeId? ReadVotedFor() => _votedFor;

    public void Update(Term term, NodeId? votedFor)
    {
        Term = term;
        VotedFor = votedFor;
    }
}