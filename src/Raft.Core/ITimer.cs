namespace Raft.Core;

public interface ITimer
{
    void Start();
    void Reset();
    void Stop();
    event Action Timeout;
}