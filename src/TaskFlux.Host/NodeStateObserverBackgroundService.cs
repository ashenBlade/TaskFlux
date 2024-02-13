using TaskFlux.Consensus;
using TaskFlux.Core.Commands;
using TaskFlux.Persistence;
using ILogger = Serilog.ILogger;

namespace TaskFlux.Host;

public class NodeStateObserverBackgroundService : BackgroundService
{
    private readonly RaftConsensusModule<Command, Response> _module;
    private readonly FileSystemPersistenceFacade _persistence;
    private readonly TimeSpan _interval;
    private readonly ILogger _logger;

    public NodeStateObserverBackgroundService(
        RaftConsensusModule<Command, Response> module,
        FileSystemPersistenceFacade persistence,
        TimeSpan interval,
        ILogger logger)
    {
        _module = module;
        _persistence = persistence;
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
                    "Роль: {State}; Терм: {Term}; Последняя запись лога: {LastEntry}; Запись в снапшоте: {SnapshotEntry}; Запись в логе (всего/коммит): {LogCount}/{LogCommitIndex}",
                    _module.CurrentRole, _persistence.CurrentTerm.Value, _persistence.LastEntry,
                    _persistence.Snapshot.LastApplied, _persistence.Log.LastRecordIndex.Value,
                    _persistence.CommitIndex.Value);
                await Task.Delay(_interval, token);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}