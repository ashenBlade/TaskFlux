using Serilog;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Core;

namespace TaskFlux.Consensus.State;

/// <summary>
/// Фоновая задача для получения голоса у другого узла
/// </summary>
internal class PeerElectorBackgroundJob<TCommand, TResponse> : IBackgroundJob
{
    public NodeId NodeId => _peer.Id;
    private readonly ILogger _logger;

    private readonly Term _term;
    private readonly LogEntryInfo _lastEntry;
    private readonly NodeId _nodeId;

    /// <summary>
    /// Узел, голос которого мы пытаемся получить
    /// </summary>
    private readonly IPeer _peer;

    /// <summary>
    /// Координатор для отслеживания состояния выборов и при достижении нужного количества голосов перейти в новое состояние.
    /// В случае, если узел проголосовал "За", то это нужно указать в мониторе.
    /// </summary>
    private readonly ElectionCoordinator<TCommand, TResponse> _coordinator;

    public PeerElectorBackgroundJob(
        Term term,
        LogEntryInfo lastEntry,
        NodeId nodeId,
        IPeer peer,
        ElectionCoordinator<TCommand, TResponse> coordinator,
        ILogger logger)
    {
        _term = term;
        _lastEntry = lastEntry;
        _nodeId = nodeId;
        _peer = peer;
        _coordinator = coordinator;
        _logger = logger;
    }


    public void Run(CancellationToken token)
    {
        try
        {
            RunQuorum(token);
        }
        catch (OperationCanceledException)
        {
            _logger.Debug("Сбор кворума прерван - задача отменена");
        }
        catch (Exception unhandled)
        {
            _logger.Fatal(unhandled, "Поймано необработанное исключение во время запуска кворума");
            throw;
        }
    }

    /// <summary>
    /// Запустить раунды кворума и попытаться получить большинство голосов.
    /// Выполняется в фоновом потоке
    /// </summary>
    /// <remarks>
    /// Дополнительные раунды нужны, когда какой-то узел не отдал свой голос.
    /// Всем отправившим ответ узлам (отдавшим голос или нет) запросы больше не посылаем.
    /// Грубо говоря, этот метод работает пока все узлы не ответят
    /// </remarks>
    private void RunQuorum(CancellationToken token)
    {
        var request = new RequestVoteRequest(_nodeId, _term, _lastEntry);
        var response = _peer.SendRequestVote(request, token);
        if (response.VoteGranted)
        {
            _coordinator.Vote();
        }
        else if (_term < response.CurrentTerm)
        {
            _coordinator.SignalGreaterTerm(response.CurrentTerm);
        }
    }

    public override string ToString()
    {
        return $"PeerElectionBackgroundJob(NodeId={_peer.Id.Id})";
    }
}