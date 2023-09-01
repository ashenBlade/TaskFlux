namespace Consensus.Raft;

public interface ITimer : IDisposable
{
    void Start();
    void Stop();
    event Action Timeout;
}