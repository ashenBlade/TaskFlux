using Raft.Core;

namespace Raft.Storage.InMemory;

public class InMemoryMetadataStorage: IMetadataStorage
{
    public Term Term { get; private set; }
    public NodeId? VotedFor { get; private set; }
    public InMemoryMetadataStorage(Term lastTerm, NodeId? votedFor)
    {
        Term = lastTerm;
        VotedFor = votedFor;
    }

    public void Update(Term term, NodeId? votedFor)
    {
        Term = term;
        VotedFor = votedFor;
    }
}