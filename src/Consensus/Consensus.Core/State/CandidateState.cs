using Consensus.Core.Commands.AppendEntries;
using Consensus.Core.Commands.InstallSnapshot;
using Consensus.Core.Commands.RequestVote;
using Consensus.Core.Commands.Submit;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Core.State;

public class CandidateState<TCommand, TResponse> : ConsensusModuleState<TCommand, TResponse>
{
    public override NodeRole Role => NodeRole.Candidate;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;

    internal CandidateState(IConsensusModule<TCommand, TResponse> consensusModule, ILogger logger)
        : base(consensusModule)
    {
        _logger = logger;
        _cts = new();
    }

    public override void Initialize()
    {
        ElectionTimer.Timeout += OnElectionTimerTimeout;
        BackgroundJobQueue.EnqueueInfinite(RunQuorum, _cts.Token);
    }

    private async Task<RequestVoteResponse?[]> SendRequestVotes(List<IPeer> peers, CancellationToken token)
    {
        // Отправляем запрос всем пирам
        var request = new RequestVoteRequest(CandidateId: Id,
            CandidateTerm: CurrentTerm, LastLogEntryInfo: PersistenceManager.LastEntry);

        var requests = new Task<RequestVoteResponse?>[peers.Count];
        for (var i = 0; i < peers.Count; i++)
        {
            requests[i] = peers[i].SendRequestVote(request, token);
        }

        return await Task.WhenAll(requests);
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
    private async Task RunQuorum()
    {
        try
        {
            await RunQuorumInner(_cts.Token);
        }
        catch (TaskCanceledException)
        {
            _logger.Debug("Сбор кворума прерван - задача отменена");
        }
        catch (ObjectDisposedException)
        {
            _logger.Verbose("Источник токенов удален во время отправки запросов");
        }
    }

    private async Task RunQuorumInner(CancellationToken token)
    {
        _logger.Debug("Запускаю кворум для получения большинства голосов");
        var leftPeers = new List<IPeer>(PeerGroup.Peers.Count);
        var term = CurrentTerm;
        leftPeers.AddRange(PeerGroup.Peers);

        var notResponded = new List<IPeer>();
        var votes = 0;
        _logger.Debug("Начинаю раунд кворума для терма {Term}. Отправляю запросы на узлы: {Peers}", term,
            leftPeers.Select(x => x.Id));
        while (!QuorumReached())
        {
            var responses = await SendRequestVotes(leftPeers, token);
            if (token.IsCancellationRequested)
            {
                _logger.Debug("Операция была отменена во время отправки запросов. Завершаю кворум");
                return;
            }

            for (var i = 0; i < responses.Length; i++)
            {
                var response = responses[i];
                if (response is null)
                {
                    notResponded.Add(leftPeers[i]);
                    _logger.Verbose("Узел {NodeId} не вернул ответ", leftPeers[i].Id);
                }
                else if (response.VoteGranted)
                {
                    votes++;
                    _logger.Verbose("Узел {NodeId} отдал голос за", leftPeers[i].Id);
                }
                else if (CurrentTerm < response.CurrentTerm)
                {
                    _logger.Verbose("Узел {NodeId} имеет более высокий Term. Перехожу в состояние Follower",
                        leftPeers[i].Id);
                    _cts.Cancel();

                    var followerState = ConsensusModule.CreateFollowerState();
                    if (ConsensusModule.TryUpdateState(followerState, this))
                    {
                        ConsensusModule.ElectionTimer.Start();
                        ConsensusModule.PersistenceManager.UpdateState(response.CurrentTerm, null);
                    }

                    return;
                }
                else
                {
                    _logger.Verbose("Узел {NodeId} не отдал голос за", leftPeers[i].Id);
                }
            }

            ( leftPeers, notResponded ) = ( notResponded, leftPeers );
            notResponded.Clear();

            if (leftPeers.Count == 0 && !QuorumReached())
            {
                _logger.Debug(
                    "Кворум не достигнут и нет узлов, которым можно послать запросы. Дожидаюсь завершения Election Timeout");
                return;
            }
        }

        _logger.Debug("Кворум собран. Получено {VotesCount} голосов. Посылаю команду перехода в состояние Leader",
            votes);

        if (token.IsCancellationRequested)
        {
            _logger.Debug("Токен был отменен. Команду перехода в Leader не посылаю");
            return;
        }

        var leaderState = ConsensusModule.CreateLeaderState();
        if (ConsensusModule.TryUpdateState(leaderState, this))
        {
            ConsensusModule.HeartbeatTimer.Start();
            ConsensusModule.ElectionTimer.Stop();
        }

        bool QuorumReached()
        {
            return PeerGroup.IsQuorumReached(votes);
        }
    }

    private void OnElectionTimerTimeout()
    {
        ElectionTimer.Timeout -= OnElectionTimerTimeout;

        _logger.Debug("Сработал Election Timeout. Перехожу в новый терм");

        var candidateState = ConsensusModule.CreateCandidateState();
        if (ConsensusModule.TryUpdateState(candidateState, this))
        {
            ConsensusModule.ElectionTimer.Stop();
            ConsensusModule.PersistenceManager.UpdateState(ConsensusModule.CurrentTerm.Increment(), ConsensusModule.Id);
            ConsensusModule.ElectionTimer.Start();
        }
    }


    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        if (request.Term < CurrentTerm)
        {
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        if (CurrentTerm < request.Term)
        {
            ElectionTimer.Start();
            ConsensusModule.PersistenceManager.UpdateState(request.Term, null);
            CurrentState = ConsensusModule.CreateFollowerState();
        }

        if (PersistenceManager.Contains(request.PrevLogEntryInfo) is false)
        {
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        if (0 < request.Entries.Count)
        {
            PersistenceManager.InsertRange(request.Entries, request.PrevLogEntryInfo.Index + 1);
        }

        if (PersistenceManager.CommitIndex < request.LeaderCommit)
        {
            var lastCommitIndex = Math.Min(request.LeaderCommit, PersistenceManager.LastEntry.Index);
            PersistenceManager.Commit(lastCommitIndex);
            var notApplied = PersistenceManager.GetNotApplied();
            if (0 < notApplied.Count)
            {
                foreach (var entry in notApplied)
                {
                    var command = CommandSerializer.Deserialize(entry.Data);
                    StateMachine.ApplyNoResponse(command);
                }
            }

            PersistenceManager.SetLastApplied(lastCommitIndex);
        }

        return AppendEntriesResponse.Ok(CurrentTerm);
    }

    public override SubmitResponse<TResponse> Apply(SubmitRequest<TCommand> request)
    {
        if (request.Descriptor.IsReadonly)
        {
            var response = StateMachine.Apply(request.Descriptor.Command);
            return SubmitResponse<TResponse>.Success(response, false);
        }

        return SubmitResponse<TResponse>.NotLeader;
    }

    public override RequestVoteResponse Apply(RequestVoteRequest request)
    {
        // Мы в более актуальном Term'е
        if (request.CandidateTerm < CurrentTerm)
        {
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
        }

        if (CurrentTerm < request.CandidateTerm)
        {
            ConsensusModule.PersistenceManager.UpdateState(request.CandidateTerm, request.CandidateId);
            ElectionTimer.Start();
            CurrentState = ConsensusModule.CreateFollowerState();
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }

        var canVote =
            // Ранее не голосовали
            VotedFor is null
          ||
            // Текущий лидер/кандидат посылает этот запрос (почему бы не согласиться)
            VotedFor == request.CandidateId;

        // Отдать свободный голос можем только за кандидата 
        if (canVote
          &&
            // У которого лог в консистентном с нашим состоянием
            !PersistenceManager.Conflicts(request.LastLogEntryInfo))
        {
            ElectionTimer.Start();
            ConsensusModule.PersistenceManager.UpdateState(request.CandidateTerm, request.CandidateId);
            CurrentState = ConsensusModule.CreateFollowerState();

            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }

        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
    }

    public override void Dispose()
    {
        try
        {
            _cts.Cancel();
            _cts.Dispose();
        }
        catch (ObjectDisposedException)
        {
        }

        ElectionTimer.Timeout -= OnElectionTimerTimeout;
    }

    public override InstallSnapshotResponse Apply(InstallSnapshotRequest request, CancellationToken token)
    {
        if (request.Term < CurrentTerm)
        {
            return new InstallSnapshotResponse(CurrentTerm);
        }

        var followerState = ConsensusModule.CreateFollowerState();
        return followerState.Apply(request, token);
    }
}