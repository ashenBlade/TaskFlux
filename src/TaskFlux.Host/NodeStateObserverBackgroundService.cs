using Microsoft.Extensions.Hosting;
using Serilog;
using TaskFlux.Consensus;
using TaskFlux.Core.Commands;

namespace TaskFlux.Host;

public class NodeStateObserverBackgroundService : BackgroundService
{
    private readonly RaftConsensusModule<Command, Response> _module;
    private readonly TimeSpan _interval;
    private readonly ILogger _logger;

    public NodeStateObserverBackgroundService(
        RaftConsensusModule<Command, Response> module,
        TimeSpan interval,
        ILogger logger)
    {
        _module = module;
        _interval = interval;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken token)
    {
        try
        {
            while (token.IsCancellationRequested is false)
            {
                _logger.Information(
                    "Состояние: {State}; Терм {Term}; Последняя запись лога: {@LastEntry}; Голос За: {VotedFor}",
                    _module.CurrentRole, _module.CurrentTerm,
                    _module.Persistence.LastEntry, _module.VotedFor);
                await Task.Delay(_interval, token);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}