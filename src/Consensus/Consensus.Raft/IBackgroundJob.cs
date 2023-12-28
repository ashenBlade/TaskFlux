using TaskFlux.Models;

namespace Consensus.Raft;

public interface IBackgroundJob
{
    public NodeId NodeId { get; }
    public void Run(CancellationToken token);
}