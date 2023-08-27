using Consensus.Raft.Persistence.Metadata;
using TaskFlux.Core;

namespace Consensus.Raft.Tests;

public class StubMetadataStorage : IMetadataStorage
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
        get => _term;
    }

    private NodeId? _votedFor;

    public NodeId? VotedFor
    {
        set => _votedFor = value;
        get => _votedFor;
    }

    public void Update(Term term, NodeId? votedFor)
    {
        Term = term;
        VotedFor = votedFor;
    }
}