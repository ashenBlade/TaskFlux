using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
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

    public RaftServer(ILogger logger)
    {
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken token)
    {
        _logger.Information("Сервер Raft запускается. Создаю узел");
        using var electionTimer = new SystemTimersTimer(TimeSpan.FromSeconds(5));
        using var heartbeatTimer = new SystemTimersTimer(TimeSpan.FromSeconds(1));

        var tcs = new TaskCompletionSource();
        await using var _ = token.Register(() => tcs.SetCanceled(token));

        _logger.Information("Запускаю сервер Raft");
        var responseTimeout = TimeSpan.FromSeconds(1);
        var peerGroup = new PeerGroup(
            Enumerable.Range(2, 2)
                      .Select(i => (IPeer) new LambdaPeer(i, async r =>
                       {
                           await Task.Delay(responseTimeout);
                           return HeartbeatResponse.Ok(r.Term);
                       }, async r =>
                       {
                           await Task.Delay(responseTimeout);
                           return new RequestVoteResponse()
                           {
                               VoteGranted = true,
                               CurrentTerm = new Term(1)
                           };
                       }))
                      .ToArray());
        var node = new Node(new PeerId(1), peerGroup);
        
        var log = new InMemoryLog();
        var logger = _logger.ForContext<RaftStateMachine>();
        var jobQueue = new TaskJobQueue();
        using var stateMachine = RaftStateMachine.Start(node, logger, electionTimer, heartbeatTimer, jobQueue, log);
        
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