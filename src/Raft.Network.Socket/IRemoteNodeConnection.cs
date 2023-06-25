namespace Raft.Network.Socket;

public interface IRemoteNodeConnection: INodeConnection
{
    public ValueTask DisconnectAsync(CancellationToken token = default);
    public ValueTask ConnectAsync(CancellationToken token = default);
}