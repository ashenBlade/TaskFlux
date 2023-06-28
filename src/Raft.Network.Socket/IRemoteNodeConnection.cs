namespace Raft.Network.Socket;

public interface IRemoteNodeConnection: INodeConnection
{
    public ValueTask DisconnectAsync(CancellationToken token = default);
    
    /// <summary>
    /// Подключиться к удаленному хосту
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns><c>true</c> - соединение установлено, <c>false</c> - соединение не установлено из-за сетевых проблем</returns>
    public ValueTask<bool> ConnectAsync(CancellationToken token = default);
}