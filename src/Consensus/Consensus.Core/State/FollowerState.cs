using System.Diagnostics;
using Consensus.Core.Commands;
using Consensus.Core.Commands.AppendEntries;
using Consensus.Core.Commands.InstallSnapshot;
using Consensus.Core.Commands.RequestVote;
using Consensus.Core.Commands.Submit;
using Consensus.Core.Log;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Core.State;

public class FollowerState<TCommand, TResponse> : ConsensusModuleState<TCommand, TResponse>
{
    public override NodeRole Role => NodeRole.Follower;
    private readonly ILogger _logger;

    internal FollowerState(IConsensusModule<TCommand, TResponse> consensusModule, ILogger logger)
        : base(consensusModule)
    {
        _logger = logger;
    }

    public override void Initialize()
    {
        ElectionTimer.Timeout += OnElectionTimerTimeout;
    }

    public override RequestVoteResponse Apply(RequestVoteRequest request)
    {
        _logger.Verbose("Получен RequestVote");
        ElectionTimer.Reset();

        // Мы в более актуальном Term'е
        if (request.CandidateTerm < CurrentTerm)
        {
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
        }

        if (CurrentTerm < request.CandidateTerm)
        {
            _logger.Debug("Получен RequestVote с большим термом {MyTerm} < {NewTerm}. Перехожу в Follower", CurrentTerm,
                request.CandidateTerm);
            ConsensusModule.UpdateState(request.CandidateTerm, request.CandidateId);
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
            !Log.Conflicts(request.LastLogEntryInfo))
        {
            _logger.Debug(
                "Получен RequestVote от узла за которого можем проголосовать. Id узла {NodeId}, Терм узла {Term}. Обновляю состояние",
                request.CandidateId.Value, request.CandidateTerm.Value);
            ConsensusModule.UpdateState(request.CandidateTerm, request.CandidateId);

            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }

        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
    }

    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        ElectionTimer.Reset();

        if (request.Term < CurrentTerm)
        {
            // Лидер устрел
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        if (CurrentTerm < request.Term)
        {
            // Мы отстали от общего состояния (старый терм)
            ConsensusModule.UpdateState(request.Term, null);
        }

        if (Log.Contains(request.PrevLogEntryInfo) is false)
        {
            // Префиксы закомиченных записей лога не совпадают 
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        if (0 < request.Entries.Count)
        {
            // Если это не Heartbeat, то применить новые команды
            Log.InsertRange(request.Entries, request.PrevLogEntryInfo.Index + 1);
        }

        Debug.Assert(Log.CommitIndex <= request.LeaderCommit,
            $"Индекс коммита лидера не должен быть меньше индекса коммита последователя. Индекс лидера: {request.LeaderCommit}. Индекс последователя: {Log.CommitIndex}");

        if (Log.CommitIndex == request.LeaderCommit)
        {
            return AppendEntriesResponse.Ok(CurrentTerm);
        }

        // В случае, если какие-то записи были закоммичены лидером, то сделать то же самое у себя.

        // Коммитим записи по индексу лидера
        Log.Commit(request.LeaderCommit);

        // Закоммиченные записи можно уже применять к машине состояний 
        var notApplied = Log.GetNotApplied();
        var lastTerm = Term.Start;
        foreach (var entry in notApplied)
        {
            var command = CommandSerializer.Deserialize(entry.Data);
            StateMachine.ApplyNoResponse(command);
            lastTerm = entry.Term;
        }

        // После применения команды, обновляем индекс последней примененной записи.
        // Этот индекс обновляем сразу, т.к. 
        // 1. Если возникнет исключение в работе, то это означает неправильную работу самого приложения, а не бизнес-логики
        // 2. Эта операция сразу сбрасывает данные на диск (Flush) - дорого
        Log.SetLastApplied(request.LeaderCommit);

        if (MaxLogFileSize < Log.LogFileSize)
        {
            // 1. Получить снапшот от машины состояний
            var snapshot = StateMachine.GetSnapshot();
            // 2. Создать временный файл снапшота

            // Последняя примененная запись - последняя закомиченная запись 
            var snapshotLastEntryInfo = new LogEntryInfo(lastTerm, Log.CommitIndex);

            Log.SaveSnapshot(snapshotLastEntryInfo, snapshot, CancellationToken.None);
            // 3. Сбросить все данные на него
            // 4. Заменить файл снапшота на новый (переименовать)
            // 5. Очистить файл лога

            throw new NotImplementedException("Создание снапшота еще не реализовано");
        }

        return AppendEntriesResponse.Ok(CurrentTerm);
    }

    /// <summary>
    /// Максимальный размер файла лога
    /// </summary>
    /// <remarks>Это значение по умолчанию. В будущем, может стать параметром</remarks>
    private const ulong MaxLogFileSize = 16    // Мб 
                                       * 1024  // Кб
                                       * 1024; // б

    public override SubmitResponse<TResponse> Apply(SubmitRequest<TCommand> request)
    {
        if (!request.Descriptor.IsReadonly)
        {
            return SubmitResponse<TResponse>.NotLeader;
        }

        var response = StateMachine.Apply(request.Descriptor.Command);
        return SubmitResponse<TResponse>.Success(response, false);
    }

    private void OnElectionTimerTimeout()
    {
        _logger.Debug("Сработал Election Timeout. Перехожу в состояние Candidate");
        CommandQueue.Enqueue(
            new MoveToCandidateAfterElectionTimerTimeoutCommand<TCommand, TResponse>(this, ConsensusModule));
    }

    public override void Dispose()
    {
        ElectionTimer.Timeout -= OnElectionTimerTimeout;
    }

    public override InstallSnapshotResponse Apply(InstallSnapshotRequest request)
    {
        throw new NotImplementedException();
    }
}