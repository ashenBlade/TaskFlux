using TaskFlux.Core;

namespace Consensus.Raft.Persistence.Metadata.Decorators;

public class ExclusiveAccessMetadataStorageDecorator : IMetadataStorage
{
    private readonly IMetadataStorage _storage;
    private SpinLock _lock = new();

    public ExclusiveAccessMetadataStorageDecorator(IMetadataStorage storage)
    {
        _storage = storage;
    }

    public Term Term
    {
        get
        {
            var acquired = false;
            try
            {
                _lock.Enter(ref acquired);
                return _storage.Term;
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

    public NodeId? VotedFor
    {
        get
        {
            var acquired = false;
            try
            {
                _lock.Enter(ref acquired);
                return _storage.VotedFor;
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