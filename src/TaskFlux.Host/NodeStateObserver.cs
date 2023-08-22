using Consensus.Core;
using Serilog;
using TaskFlux.Commands;

// ReSharper disable ContextualLoggerProblem

namespace TaskFlux.Host;

public class NodeStateObserver
{
    private readonly IConsensusModule<Command, Result> _consensusModule;
    private readonly ILogger _logger;

    public NodeStateObserver(IConsensusModule<Command, Result> consensusModule, ILogger logger)
    {
        _consensusModule = consensusModule;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken token)
    {
        try
        {
            while (token.IsCancellationRequested is false)
            {
                _logger.Information("Состояние: {State}; Терм {Term}; Последняя запись лога: {@LastEntry}; Голос За: {VotedFor}", _consensusModule.CurrentRole, _consensusModule.CurrentTerm, _consensusModule.PersistenceManager.LastEntry, _consensusModule.VotedFor);
                await Task.Delay(TimeSpan.FromSeconds(2.5), token);
            }
        }
        catch (TaskCanceledException)
        { }
    }
}