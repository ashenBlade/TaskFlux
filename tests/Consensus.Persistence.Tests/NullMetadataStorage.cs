using Consensus.Core;
using TaskFlux.Core;

namespace Consensus.Persistence.Tests;

public class NullMetadataStorage : IMetadataStorage
{
    public static readonly NullMetadataStorage Instance = new();

    public Term ReadTerm()
    {
        return Term.Start;
    }

    public NodeId? ReadVotedFor()
    {
        return null;
    }

    public void Update(Term term, NodeId? votedFor)
    {
    }
}