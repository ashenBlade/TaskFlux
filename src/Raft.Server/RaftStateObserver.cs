using Raft.Core.StateMachine;
using Serilog;

// ReSharper disable ContextualLoggerProblem

namespace Raft.Server;

public partial class RaftStateObserver
{
    private readonly RaftStateMachine _raft;
    private readonly ILogger _logger;


    public RaftStateObserver(RaftStateMachine raft, ILogger logger)
    {
        _raft = raft;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken token)
    {
        try
        {
            while (token.IsCancellationRequested is false)
            {
                _logger.Information("Состояние: {State}; Терм {Term}", _raft.CurrentRole, _raft.CurrentTerm);
                await Task.Delay(TimeSpan.FromSeconds(5), token);
            }
        }
        catch (TaskCanceledException)
        { }
    }
}