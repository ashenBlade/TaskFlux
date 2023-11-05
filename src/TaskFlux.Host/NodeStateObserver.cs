using Consensus.Raft;
using Serilog;
using TaskFlux.Commands;

// ReSharper disable ContextualLoggerProblem

namespace TaskFlux.Host;

public class NodeStateObserver
{
    private readonly IConsensusModule<Command, Response> _consensusModule;
    private readonly ILogger _logger;

    public NodeStateObserver(IConsensusModule<Command, Response> consensusModule, ILogger logger)
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
                _logger.Information(
                    "Состояние: {State}; Терм {Term}; Последняя запись лога: {@LastEntry}; Голос За: {VotedFor}",
                    _consensusModule.CurrentRole, _consensusModule.CurrentTerm,
                    _consensusModule.PersistenceFacade.LastEntry, _consensusModule.VotedFor);
                await Task.Delay(TimeSpan.FromSeconds(2.5), token);
            }
        }
        catch (TaskCanceledException)
        {
        }
    }
}