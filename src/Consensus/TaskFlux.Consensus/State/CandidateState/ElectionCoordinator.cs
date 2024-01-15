using Serilog;

namespace TaskFlux.Consensus.State;

internal class ElectionCoordinator<TCommand, TResponse>
{
    /// <summary>
    /// Кандидат, на которого мы работаем
    /// </summary>
    private readonly CandidateState<TCommand, TResponse> _state;

    private readonly ILogger _logger;

    private PeerGroup Peers => _state.RaftConsensusModule.PeerGroup;
    private RaftConsensusModule<TCommand, TResponse> Module => _state.RaftConsensusModule;

    /// <summary>
    /// Терм кандидата (тот в котором зашли в это состояние)
    /// </summary>
    private readonly Term _term;

    /// <summary>
    /// Количество проголосовавших "За" узлов.
    /// При достижении определенного количества, происходит переход в состояние лидера
    /// </summary>
    private int _voted;

    public ElectionCoordinator(CandidateState<TCommand, TResponse> state, ILogger logger, Term term)
    {
        _term = term;
        _state = state;
        _logger = logger;
    }

    /// <summary>
    /// Указать, что какой-то узел отдал свой голос 
    /// </summary>
    public void Vote()
    {
        var votes = Interlocked.Increment(ref _voted);
        if (IsQuorumThresholdPassed(votes))
        {
            _logger.Information("Количество полученных голосов превысило предел. Становлюсь лидером");
            var leader = Module.CreateLeaderState();
            if (Module.TryUpdateState(leader, _state))
            {
                _logger.Debug("Превышен порог необходимых голосов. Становлюсь лидером");
                Module.PersistenceFacade.UpdateState(_term.Increment(), null);
            }
            else
            {
                _logger.Debug("Стать лидером не удалось: состояние изменилось");
            }
        }
    }

    /// <summary>
    /// Проверить, что полученное число голосов прошло порог необходимого для выигрыша кворума
    /// </summary>
    /// <param name="votes">Полученное количество голосов</param>
    /// <returns><c>true</c> - порог пройден, <c>false</c> - иначе</returns>
    /// <remarks>
    /// <c>true</c> возвращается только когда получено пороговое значение, но не другое.
    /// Например, если в кластере 5 узлов, то <c>true</c> вернется только при переданном 2, а на 3 и 4 будет возвращено <c>false</c>.
    /// Состояние должно быть изменено только 1 раз.
    /// </remarks>
    private bool IsQuorumThresholdPassed(int votes) =>
        Peers.IsQuorumReached(votes) && !Peers.IsQuorumReached(votes - 1);

    public void SignalGreaterTerm(Term term)
    {
        var follower = Module.CreateFollowerState();
        if (Module.TryUpdateState(follower, _state))
        {
            Module.PersistenceFacade.UpdateState(term, null);
        }
    }
}