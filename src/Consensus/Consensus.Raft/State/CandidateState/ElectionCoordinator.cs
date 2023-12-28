namespace Consensus.Raft.State;

internal class ElectionCoordinator<TCommand, TResponse>
{
    /// <summary>
    /// Кандидат, на которого мы работаем
    /// </summary>
    private readonly CandidateState<TCommand, TResponse> _state;

    private PeerGroup Peers => _state.RaftConsensusModule.PeerGroup;
    private IRaftConsensusModule<TCommand, TResponse> Module => _state.RaftConsensusModule;

    /// <summary>
    /// Терм кандидата (тот в котором зашли в это состояние)
    /// </summary>
    private readonly Term _term;

    /// <summary>
    /// Количество проголосовавших "За" узлов.
    /// При достижении определенного количества, происходит переход в состояние лидера
    /// </summary>
    private int _voted;

    public ElectionCoordinator(CandidateState<TCommand, TResponse> state, Term term)
    {
        _term = term;
        _state = state;
    }

    /// <summary>
    /// Указать, что какой-то узел отдал голос за нас
    /// </summary>
    public void Vote()
    {
        var votes = Interlocked.Increment(ref _voted);
        if (IsQuorumThresholdPassed(votes))
        {
            var leader = Module.CreateLeaderState();
            if (Module.TryUpdateState(leader, _state))
            {
                Module.PersistenceFacade.UpdateState(_term.Increment(), null);
            }
        }
    }

    public void SignalGreaterTerm(Term term)
    {
        var follower = Module.CreateFollowerState();
        if (Module.TryUpdateState(follower, _state))
        {
            Module.PersistenceFacade.UpdateState(term, null);
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
}