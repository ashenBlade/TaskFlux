using Consensus.Core;

namespace Consensus.Storage.File.Decorators;

public class ExclusiveAccessMetadataStorageDecorator: IMetadataStorage
{
    private readonly IMetadataStorage _storage;
    private SpinLock _lock = new();

    public ExclusiveAccessMetadataStorageDecorator(IMetadataStorage storage)
    {
        _storage = storage;
    }

    public Term ReadTerm()
    {
        var acquired = false;
        try
        {
            _lock.Enter(ref acquired);
            return _storage.ReadTerm();
        }
        finally
        {
            if (acquired)
            {
                _lock.Exit();
            }
        }
    }

    public NodeId? ReadVotedFor()
    {
        var acquired = false;
        try
        {
            _lock.Enter(ref acquired);
            return _storage.ReadVotedFor();
        }
        finally
        {
            if (acquired)
            {
                _lock.Exit();
            }
        }
    }

    public void Update(Term term, NodeId? votedFor)
    {
        
        var acquired = false;
        try
        {
            _lock.Enter(ref acquired);
            _storage.Update(term, votedFor);
        }
        finally
        {
            if (acquired)
            {
                _lock.Exit();
            }
        }
    }
}