using System.Net.Sockets;
using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Node;
using Raft.Network;
using Serilog;

namespace Raft.Server;

public record NodeConnectionProcessor(NodeId Id, TcpClient Client, RaftNode Node, ILogger Logger): IDisposable
{
    public CancellationTokenSource CancellationTokenSource { get; init; }
    public bool Stopped { get; private set; } = false;

    public async Task ProcessClientBackground()
    {
        var token = CancellationTokenSource.Token;
        var (id, client, stateMachine, logger) = this;
        var network = client.GetStream();
        using var memory = new MemoryStream();
        var buffer = new byte[2048];
        logger.Debug("Начинаю обрабатывать запросы клиента {Id} по адресу {@Address}", id, client.Client.RemoteEndPoint);
        while (token.IsCancellationRequested is false)
        {
            try
            {
                int read;
                while ((read = await network.ReadAsync(buffer, token)) > 0)
                {
                    memory.Write(buffer, 0, read);
                    if (read < buffer.Length)
                    {
                        break;
                    }
                }

                logger.Debug("От клиента получен запрос. Начинаю обработку");
                var data = memory.ToArray();

                if (data.Length == 0)
                {
                    logger.Debug("Клиент {Id} закрыл соединение", id);
                    CancellationTokenSource.Cancel();
                    return;
                }
                        
                var marker = data[0];
                byte[]? responseBuffer = null;
                switch (marker)
                {
                    case (byte) RequestType.RequestVote:
                        logger.Debug("Получен RequestVote. Десериализую");
                        var requestVoteRequest = Serializers.RequestVoteRequest.Deserialize(data);
                        logger.Verbose("RequestVoteRequest: {Request}", requestVoteRequest);
                        logger.Debug("RequestVote десериализован. Отправляю команду машине");
                        var requestVoteResponse = stateMachine.Handle(requestVoteRequest);
                        logger.Debug("Команда обработана. Сериализую ответ");
                        logger.Verbose("RequestVoteResponse: {Response}", requestVoteResponse);
                        responseBuffer = Serializers.RequestVoteResponse.Serialize(requestVoteResponse);
                        break;
                    case (byte) RequestType.AppendEntries:
                        logger.Debug("Получен AppendEntries. Десериализую в Heartbeat");
                        var heartbeatRequest = Serializers.HeartbeatRequest.Deserialize(buffer);
                        logger.Verbose("HeartbeatRequest: {Request}", heartbeatRequest);
                        logger.Debug("Heartbeat десериализован. Отправляю команду машине");
                        var heartbeatResponse = stateMachine.Handle(heartbeatRequest);
                        logger.Debug("Команда обработана. Сериализую ответ");
                        logger.Verbose("HeartbeatResponse: {Response}", heartbeatResponse);
                        responseBuffer = Serializers.HeartbeatResponse.Serialize(heartbeatResponse);
                        break;
                }
                    
                memory.Position = 0;
                memory.SetLength(0);

                if (responseBuffer is null)
                {
                    logger.Error("Не удалось сериализовать ответ. Закрываю соединение с {@Address}", client.Client.RemoteEndPoint);
                    client.Close();
                    Stopped = true;
                    continue;
                }
                    
                logger.Debug("Отправляю ответ клиенту");
                await network.WriteAsync(responseBuffer, token);
                logger.Debug("Ответ отправлен");
            }
            catch (Exception exception)
            {
                logger.Error(exception, "Поймано необработанное исключение при работе с узлом {PeerId}", id);
                Stopped = true;
            }
        }
    }

    public void Dispose()
    {
        try
        {
            CancellationTokenSource.Cancel();
        }
        catch (ObjectDisposedException)
        { }
            
        Client.Dispose();
        CancellationTokenSource.Dispose();
    }
}