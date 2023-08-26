namespace Consensus.Raft;

public interface ITimer
{
    void Start();
    void Reset();
    void Stop();
    event Action Timeout;
}