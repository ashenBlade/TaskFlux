using System.Threading.Channels;
using Consensus.Raft.Commands.AppendEntries;

namespace Consensus.Raft.State.LeaderState;

internal record PeerProcessor<TCommand, TResponse>(LeaderState<TCommand, TResponse> State,
                                                   IPeer Peer)
{
    // Вначале инициализируем индекс на следующий после последнего.
    // Используем этот индекс, чтобы делать срез лога через него (это первый индекс среза).
    private PeerInfo Info { get; } = new(State.ConsensusModule.PersistenceFacade.LastEntry.Index + 1);

    /// <summary>
    /// Очередь для входящих команд.
    /// Используется <see cref="Channel"/>, причем ограниченный. 
    /// </summary>
    /// <remarks>
    /// Размер этой очереди - 2 элемента.
    /// </remarks>
    private Channel<LogReplicationRequest> Queue { get; } =
        Channel.CreateBounded<LogReplicationRequest>(new BoundedChannelOptions(2) {SingleReader = true});

    /// <summary>
    /// Метод для начала обработки узла
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <remarks><paramref name="token"/> может быть отменен, когда переходим в новое состояние</remarks>
    public async Task StartServingAsync(CancellationToken token)
    {
        // Ебать какая неоптимальная хуйня
        var reader = Queue.Reader;
        /*
         * В любой момент времени в очереди не может быть более 2 элементов:
         * - 1 Heartbeat - это просто сигнал к действию, много не нужно
         * - 1 Submit - только 1 максимум, т.к. приложение однопоточное
         */

        await foreach (var rs in reader.ReadAllAsync(token))
        {
            // Не проверять валидность индекса здесь (перед отправкой запроса)
            // Может случиться так, что обработчик только инициализировался и индекс отображает неверные данные

            if (token.IsCancellationRequested)
            {
                // При отменене асинхронного потока он считывает все данные до конца (не кидает исключения)
                // Этот момент нужно обработать самим
                rs.NotifyComplete();
                continue;
            }

            var success = await ProcessRequestAsync(rs, token);
            rs.NotifyComplete();
            if (!success)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Метод для обработки полученного запроса из очереди <see cref="Queue{T}"/>.
    /// </summary>
    /// <param name="synchronizer">Полученный из очереди <see cref="LogReplicationRequest"/></param>
    /// <param name="token">Токен отмены</param>
    /// <remarks>Содержит логику повторных попыток при инконсистентности лога</remarks>
    /// <returns>
    /// <c>true</c> - узел успешно обработал запрос,
    /// <c>false</c> - не удалось обработать запрос.
    /// В этом случае прекращаем работу.
    /// Например, узел вернул терм больше нашего и мы должны перейти в Follower
    /// </returns>
    private async Task<bool> ProcessRequestAsync(LogReplicationRequest synchronizer, CancellationToken token)
    {
        while (token.IsCancellationRequested is false)
        {
            if (!ConsensusModule.PersistenceFacade.TryGetFrom(Info.NextIndex, out var entries))
            {
                throw new NotImplementedException("Обработка когда снапшот хранит данные");
            }

            // 1. Отправляем запрос с текущим отслеживаемым индексом узла
            var request = new AppendEntriesRequest(Term: ConsensusModule.CurrentTerm,
                LeaderCommit: ConsensusModule.PersistenceFacade.CommitIndex,
                LeaderId: ConsensusModule.Id,
                PrevLogEntryInfo: ConsensusModule.PersistenceFacade.GetPrecedingEntryInfo(Info.NextIndex),
                Entries: entries);

            var response = await Peer.SendAppendEntries(request, token);

            // 2. Если ответ не вернулся (null) - соединение было разорвано - делаем повторную попытку с переподключением
            if (response is null)
            {
                throw new NotImplementedException("Переподключиться и попытаться заново");
            }

            // 3. Если ответ успешный 
            if (response.Success)
            {
                // 3.1. Обновить nextIndex = + кол-во Entries в запросе
                // 3.2. Обновить matchIndex = новый nextIndex - 1
                Info.Update(request.Entries.Count);

                // 3.3. Если лог не до конца был синхронизирован
                if (Info.NextIndex < synchronizer.LogIndex)
                {
                    // Заходим на новый круг и отправляем заново
                    continue;
                }

                // 3.4. Уведомляем об успешной отправке команды на узел
                return true;
            }

            // Дальше узел отказался принимать наш запрос (Success = false)
            // 4. Если вернувшийся терм больше нашего
            if (ConsensusModule.CurrentTerm < response.Term)
            {
                // 4.1. Перейти в состояние Follower
                var followerState = ConsensusModule.CreateFollowerState();
                if (ConsensusModule.TryUpdateState(followerState, State))
                {
                    ConsensusModule.ElectionTimer.Start();
                    ConsensusModule.PersistenceFacade.UpdateState(response.Term, null);
                }

                // 4.2. Закончить работу
                return false;
            }

            // 5. В противном случае у узла не синхронизирован лог 

            // 5.1. Декрементируем последние записи лога
            Info.Decrement();

            // 5.2. Идем на следующий круг
        }

        // Этот вариант возможен если токен отменен - необходимо закончить работу
        return false;
    }

    private IConsensusModule<TCommand, TResponse> ConsensusModule => State.ConsensusModule;

    public void NotifyHeartbeatTimeout()
    {
        throw new NotImplementedException();
    }

    public void NotifyAppendEntries(LogReplicationRequest synchronizer)
    {
        // Можно не проверять возвращаемое значение -
        // для Unbounded канала всегда возвращает true
        Queue.Writer.TryWrite(synchronizer);
    }

    private record RequestSynchronizer();
}