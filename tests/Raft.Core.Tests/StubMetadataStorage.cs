namespace Raft.Core.Tests;

public class StubMetadataStorage: IMetadataStorage
{
    public StubMetadataStorage(Term term, NodeId? votedFor)
    {
        Term = term;
        VotedFor = votedFor;
    }
    
    public Term Term { get; set; }
    public NodeId? VotedFor { get; set; }
    public void Update(Term term, NodeId? votedFor)
    {
        Term = term;
        VotedFor = votedFor;
    }
}