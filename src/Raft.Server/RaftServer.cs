using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Log;
using Raft.Core.Peer;
using Raft.Core.StateMachine;
using Raft.JobQueue;
using Raft.Log;
using Raft.Peer;
using Raft.Timers;
using Serilog;
// ReSharper disable ContextualLoggerProblem

namespace Raft.Server;

public class RaftServer
{
    private readonly ILogger _logger;
    private readonly IPeer[] _peers;
    private readonly ILog _log;
    private readonly IJobQueue _jobQueue;
    private readonly ITimer _electionTimer;
    private readonly ITimer _heartbeatTimer;

    public RaftServer(ILogger logger, IPeer[] peers, ILog log, IJobQueue jobQueue, ITimer electionTimer, ITimer heartbeatTimer)
    {
        _logger = logger;
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
        await using var _ = token.Register(() => tcs.SetCanceled(token));

        _logger.Information("Запускаю сервер Raft");
        var peerGroup = new PeerGroup(_peers);
        var node = new Node(new PeerId(1), peerGroup);

        using var stateMachine = RaftStateMachine.Start(node, 
            _logger.ForContext<RaftStateMachine>(),
            _electionTimer, 
            _heartbeatTimer,
            _jobQueue,
            _log);
        
        try
        {
            await tcs.Task;
        }
        catch (TaskCanceledException taskCanceled)
        {
            _logger.Information(taskCanceled, "Запрошено завершение работы приложения");
        }
        
        _logger.Information("Узел завершает работу");
        GC.KeepAlive(stateMachine);
    }
}