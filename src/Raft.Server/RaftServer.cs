using System.Net.Sockets;
using Raft.Core;
using Raft.Core.Log;
using Raft.Core.Peer;
using Raft.Core.StateMachine;
using Raft.Peer;
using Serilog;

// ReSharper disable ContextualLoggerProblem

namespace Raft.Server;

public class RaftServer
{
    private record ClientProcess(PeerId Id, TcpClient Client, RaftStateMachine StateMachine, Dictionary<PeerId, ClientProcess> Clients, ILogger Logger): IDisposable
    {
        public CancellationTokenSource CancellationTokenSource { get; init; }

        public async Task ProcessClientBackground()
        {
            var token = CancellationTokenSource.Token;
            var (id, client, stateMachine, _, logger) = this;
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
                        Clients.Remove(id);
                        CancellationTokenSource.Cancel();
                        return;
                    }
                        
                    var marker = data[0];
                    byte[]? responseBuffer = null;
                    switch (marker)
                    {
                        case (byte)RequestType.RequestVote:
                            logger.Debug("Получен RequestVote. Десериализую");
                            var requestVoteRequest = Helpers.DeserializeRequestVoteRequest(data);
                            logger.Debug("RequestVote десериализован. Отправляю команду машине");
                            var requestVoteResponse = stateMachine.Handle(requestVoteRequest, token);
                            logger.Debug("Команда обработана. Сериализую");
                            responseBuffer = Helpers.Serialize(requestVoteResponse);
                            break;
                        case (byte)RequestType.AppendEntries:
                            logger.Debug("Получен AppendEntries. Десериализую в Heartbeat");
                            var heartbeatRequest = Helpers.DeserializeHeartbeatRequest(buffer);
                            logger.Debug("Heartbeat десериализован. Отправляю команду машине");
                            var heartbeatResponse = stateMachine.Handle(heartbeatRequest, token);
                            logger.Debug("Команда обработана. Сериализую");
                            responseBuffer = Helpers.Serialize(heartbeatResponse);
                            break;
                    }
                    
                    memory.Position = 0;
                    memory.SetLength(0);

                    if (responseBuffer is null)
                    {
                        logger.Error("Не удалось сериализовать ответ. Закрываю соединение с {@Address}", client.Client.RemoteEndPoint);
                        client.Close();
                        continue;
                    }
                    
                    logger.Debug("Отправляю ответ клиенту");
                    await network.WriteAsync(responseBuffer, token);
                    logger.Debug("Ответ отправлен");
                }
                catch (Exception exception)
                {
                    logger.Error(exception, "Поймано необработанное исключение при работе с узлом {PeerId}", id);
                    Clients.Remove(id);
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

    private readonly PeerId _nodeId;
    private readonly ILogger _logger;
    private readonly ExternalConnectionsManager _connectionsManager;
    private readonly IPeer[] _peers;
    private readonly ILog _log;
    private readonly IJobQueue _jobQueue;
    private readonly ITimer _electionTimer;
    private readonly ITimer _heartbeatTimer;
    private readonly Dictionary<PeerId, ClientProcess> _peersDictionary = new();
    private volatile bool _stopped;


    public RaftServer(PeerId nodeId, ILogger logger, ExternalConnectionsManager connectionsManager, IPeer[] peers, ILog log, IJobQueue jobQueue, ITimer electionTimer, ITimer heartbeatTimer)
    {
        _nodeId = nodeId;
        _logger = logger;
        _connectionsManager = connectionsManager;
        _peers = peers;
        _log = log;
        _jobQueue = jobQueue;
        _electionTimer = electionTimer;
        _heartbeatTimer = heartbeatTimer;
    }

    public async Task RunAsync(CancellationToken token)
    {
        _logger.Information("Сервер Raft запускается. Создаю узел");
        
        var tcs = new TaskCompletionSource();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        
        await using var registration = token.Register(() =>
        {
            tcs.SetCanceled(token);
        });
        
        _logger.Information("Запускаю сервер Raft");
        var peerGroup = new PeerGroup(_peers);
        var node = new Node(_nodeId, peerGroup);

        using var stateMachine = RaftStateMachine.Start(node, 
            _logger.ForContext<RaftStateMachine>(),
            _electionTimer, 
            _heartbeatTimer,
            _jobQueue,
            _log);

        _connectionsManager.PeerConnected += ListenerOnPeerConnected;
        _ = Task.Run(async () =>
        {
            while (!_stopped)
            {
                _logger.Information("Текущее состояние: {State}, Терм {Term}", stateMachine.CurrentRole, stateMachine.Node.CurrentTerm);
                await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
            }
        }, cts.Token);
        
        try
        {
            await Task.WhenAll(tcs.Task, _connectionsManager.RunAsync(cts.Token));
        }
        catch (TaskCanceledException taskCanceled)
        {
            tcs.SetCanceled(token);
            _logger.Information(taskCanceled, "Запрошено завершение работы приложения");
        }

        _stopped = true;
        _connectionsManager.PeerConnected -= ListenerOnPeerConnected;
        
        _logger.Information("Узел завершает работу");
        GC.KeepAlive(stateMachine);

        void ListenerOnPeerConnected(object? sender, (PeerId Id, TcpClient Client) e)
        {
            
            if (_peersDictionary.TryGetValue(e.Id, out var oldClientProcess))
            {
                oldClientProcess.Dispose();
            }
            
            var clientProcess = new ClientProcess(e.Id, e.Client, stateMachine, _peersDictionary, _logger.ForContext("SourceContext", $"Обработчик Клиента {e.Id.Value}"))
            {
                CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token)
            };
            
            _peersDictionary[e.Id] = clientProcess;

            _ = clientProcess.ProcessClientBackground();
        }
    }
}