using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Peer;
using Raft.Core.StateMachine;
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
        var node = new Node(new PeerId(1))
        {
            PeerGroup = new(Enumerable.Range(2, 2)
                                      .Select(i => (IPeer)new LambdaPeer(i, async _ =>
                                       {
                                           await Task.Delay(responseTimeout);
                                           return new HeartbeatResponse();
                                       }, async r =>
                                       {
                                           await Task.Delay(responseTimeout);
                                           return new RequestVoteResponse()
                                           {
                                               VoteGranted = false,
                                               CurrentTerm = r.CandidateTerm.Increment()
                                           };
                                       }))
                                      .ToArray())
        };
        
        using var stateMachine = new RaftStateMachine(node, _logger.ForContext<RaftStateMachine>(), electionTimer, heartbeatTimer);
        
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