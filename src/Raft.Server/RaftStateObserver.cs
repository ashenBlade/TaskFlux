using Raft.Core.Node;
using Serilog;

// ReSharper disable ContextualLoggerProblem

namespace Raft.Server;

public class RaftStateObserver
{
    private readonly RaftNode _raft;
    private readonly ILogger _logger;


    public RaftStateObserver(RaftNode raft, ILogger logger)
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
                _logger.Information("Состояние: {State}; Терм {Term}; Лог: {@Entries}", _raft.CurrentRole, _raft.CurrentTerm, _raft.Log);
                await Task.Delay(TimeSpan.FromSeconds(5), token);
            }
        }
        catch (TaskCanceledException)
        { }
    }
}