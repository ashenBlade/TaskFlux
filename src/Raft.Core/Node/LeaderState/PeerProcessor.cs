using Raft.Core.Commands;
using Raft.Core.Commands.AppendEntries;

namespace Raft.Core.Node.LeaderState;


internal record PeerProcessor(LeaderState State, IPeer Peer, IRequestQueue Queue)
{
    private PeerInfo Info { get; } = new(State.Node.Log.LastEntry.Index + 1);

    /// <summary>
    /// Метод для обработки узла
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <remarks><paramref name="token"/> может быть отменен, когда переходим в новое состояние</remarks>
    public async Task StartServingAsync(CancellationToken token)
    {
        await foreach (var rs in Queue.ReadAllRequestsAsync(token))
        {
            var success = await ProcessRequestAsync(rs, token);
            rs.NotifyComplete();
            if (!success)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Метод для обработки полученного запроса из очереди <see cref="Queue"/>.
    /// </summary>
    /// <param name="synchronizer">Полученный из очереди <see cref="AppendEntriesRequestSynchronizer"/></param>
    /// <param name="token">Токен отмены</param>
    /// <remarks>Содержит логику повторных попыток при инконсистентности лога</remarks>
    /// <returns>
    /// <c>true</c> - узел успешно обработал запрос,
    /// <c>false</c> - не удалось обработать запрос.
    /// В этом случае прекращаем работу.
    /// Например, узел вернул терм больше нашего и мы должны перейти в Follower
    /// </returns>
    private async Task<bool> ProcessRequestAsync(AppendEntriesRequestSynchronizer synchronizer, CancellationToken token)
    {
        while (token.IsCancellationRequested is false)
        {
            // 1. Отправить запрос
            var request = new AppendEntriesRequest(
                Term: Node.CurrentTerm,
                LeaderCommit: Node.Log.CommitIndex,
                LeaderId: Node.Id,
                PrevLogEntryInfo: Node.Log.GetPrecedingEntryInfo(Info.NextIndex), 
                Entries: Node.Log.GetFrom(Info.NextIndex));
            AppendEntriesResponse? response;
            try
            {
                response = await Peer.SendAppendEntries(request, token);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            
            // 2. Если ответ не вернулся (null) - соединение было разорвано. Прекратить обработку
            if (response is null)
            {
                return true;
            }
            
            // 3. Если ответ успешный 
            if (response.Success)
            {
                // 3.1. Обновить nextIndex = + кол-во Entries в запросе
                // 3.2. Обновить matchIndex = новый nextIndex - 1
                Info.Update(request.Entries.Count);
                
                // 3.3. Если лог не до конца был синхронизирован
                if (Info.NextIndex < synchronizer.LogEntryIndex)
                {
                    // Заходим на новый круг и отправляем заново
                    continue;
                }
                
                // 3.4. Уведомляем об успешной отправке команды на узел
                return true;
            }
            
            // Дальше узел отказался принимать наш запрос (Success = false)
            // 4. Если вернувшийся терм больше нашего
            if (Node.CurrentTerm < response.Term)
            {
                // 4.1. Перейти в состояние Follower
                Node.CommandQueue.Enqueue(new MoveToFollowerStateCommand(response.Term, null, State, Node));
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

    private INode Node => State.Node;

    public void NotifyHeartbeatTimeout()
    {
        Queue.AddHeartbeatIfEmpty();
    }

    public void NotifyAppendEntries(AppendEntriesRequestSynchronizer synchronizer)
    {
        Queue.AddAppendEntries(synchronizer);
    }
}