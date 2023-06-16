using Raft.Core.Commands;

namespace Raft.Core;

public interface IJobQueue
{
    public void EnqueueInfinite(Func<Task> job, CancellationToken token);
}