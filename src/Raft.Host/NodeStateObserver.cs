using Raft.Core.Node;
using Serilog;

// ReSharper disable ContextualLoggerProblem

namespace Raft.Host;

public class NodeStateObserver
{
    private readonly RaftNode _node;
    private readonly ILogger _logger;


    public NodeStateObserver(RaftNode node, ILogger logger)
    {
        _node = node;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken token)
    {
        try
        {
            while (token.IsCancellationRequested is false)
            {
                _logger.Information("Состояние: {State}; Терм {Term}; Лог: {@Entries}; Голос За: {VotedFor}", _node.CurrentRole, _node.CurrentTerm, _node.Log.ReadLog(), _node.VotedFor);
                await Task.Delay(TimeSpan.FromSeconds(2.5), token);
            }
        }
        catch (TaskCanceledException)
        { }
    }
}