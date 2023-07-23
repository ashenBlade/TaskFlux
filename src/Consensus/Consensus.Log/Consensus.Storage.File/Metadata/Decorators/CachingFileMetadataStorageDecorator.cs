using Consensus.Core;
using TaskFlux.Core;

namespace Consensus.Storage.File.Decorators;

public class CachingFileMetadataStorageDecorator: IMetadataStorage
{
    private readonly IMetadataStorage _storage;
    private Term _term;
    private NodeId? _votedFor; 

    public CachingFileMetadataStorageDecorator(IMetadataStorage storage)
    {
        _storage = storage;
        _term = storage.ReadTerm();
        _votedFor = storage.ReadVotedFor();
    }

    public Term ReadTerm()
    {
        return _term;
    }

    public NodeId? ReadVotedFor()
    {
        return _votedFor;
    }

    public void Update(Term term, NodeId? votedFor)
    {
        _storage.Update(term, votedFor);
        _term = term;
        _votedFor = votedFor;
    }
}