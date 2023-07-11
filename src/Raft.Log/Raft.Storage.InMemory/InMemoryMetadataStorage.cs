using Raft.Core;

namespace Raft.Storage.InMemory;

public class InMemoryMetadataStorage: IMetadataStorage
{
    private Term _term;
    
    public Term ReadTerm() => _term;

    private NodeId? _votedFor;
    
    public NodeId? ReadVotedFor() => _votedFor;

    public InMemoryMetadataStorage(Term lastTerm, NodeId? votedFor)
    {
        _term = lastTerm;
        _votedFor = votedFor;
    }

    public void Update(Term term, NodeId? votedFor)
    {
        _term = term;
        _votedFor = votedFor;
    }
}