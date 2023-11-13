using TaskFlux.Models;

namespace Consensus.Raft.Persistence.Metadata.Decorators;

public class CachingFileMetadataStorageDecorator : IMetadataStorage
{
    private readonly IMetadataStorage _storage;
    private Term _term;
    private NodeId? _votedFor;

    public CachingFileMetadataStorageDecorator(IMetadataStorage storage)
    {
        _storage = storage;
        _term = storage.Term;
        _votedFor = storage.VotedFor;
    }

    public Term Term =>
        _term;

    public NodeId? VotedFor =>
        _votedFor;

    public void Update(Term term, NodeId? votedFor)
    {
        _storage.Update(term, votedFor);
        _term = term;
        _votedFor = votedFor;
    }
}