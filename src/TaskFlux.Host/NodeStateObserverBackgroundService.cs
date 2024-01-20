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
            var persistence = _module.Persistence;
            while (token.IsCancellationRequested is false)
            {
                _logger.Information(
                    "Роль: {State}; Терм: {Term}; Последняя запись лога: {LastEntry}; Запись в снапшоте: {SnapshotEntry}; Запись в логе (всего/коммит): {LogCount}/{LogCommitIndex}",
                    _module.CurrentRole, _module.CurrentTerm.Value, persistence.LastEntry,
                    persistence.SnapshotStorage.LastApplied, persistence.Log.Count, persistence.Log.CommitIndex + 1);
                await Task.Delay(_interval, token);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}