namespace Raft.Peer;

public interface ISocket: IDisposable
{
    public Task SendAsync(byte[] payload, CancellationToken token = default);
    public Task<int> ReadAsync(byte[] buffer, CancellationToken token = default);
}