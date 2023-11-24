using Consensus.Raft;
using Serilog;
using TaskFlux.Commands;

// ReSharper disable ContextualLoggerProblem

namespace TaskFlux.Host;

public class NodeStateObserver
{
    private readonly IRaftConsensusModule<Command, Response> _raftConsensusModule;
    private readonly ILogger _logger;

    public NodeStateObserver(IRaftConsensusModule<Command, Response> raftConsensusModule, ILogger logger)
    {
        _raftConsensusModule = raftConsensusModule;
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
                    _raftConsensusModule.CurrentRole, _raftConsensusModule.CurrentTerm,
                    _raftConsensusModule.PersistenceFacade.LastEntry, _raftConsensusModule.VotedFor);
                await Task.Delay(TimeSpan.FromSeconds(2.5), token);
            }
        }
        catch (TaskCanceledException)
        {
        }
    }
}