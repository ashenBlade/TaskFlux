using TaskFlux.Core;

namespace TaskFlux.Consensus;

public interface IBackgroundJob
{
    public NodeId NodeId { get; }
    public void Run(CancellationToken token);
}