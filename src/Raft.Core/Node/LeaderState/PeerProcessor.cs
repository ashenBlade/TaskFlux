using System.Threading.Channels;
using Raft.Core.Commands;
using Raft.Core.Commands.AppendEntries;

namespace Raft.Core.Node.LeaderState;


internal record PeerProcessor(LeaderState State, IPeer Peer, IRequestQueue Queue)
{
    private PeerInfo Info { get; } = new(State.Node.Log.LastLogEntryInfo.Index);
    private bool IsBusy { get; set; }
    
    private readonly record struct OperationScope(PeerProcessor? Processor): IDisposable
    {
        public static OperationScope Begin(PeerProcessor processor)
        {
            processor.IsBusy = true;
            return new OperationScope(processor);
        }

        public void Dispose()
        {
            if (Processor is not null)
            {
                Processor.IsBusy = false;
            }
        }
    }
    
    private readonly Channel<AppendEntriesRequestSynchronizer> _channel =
        Channel.CreateUnbounded<AppendEntriesRequestSynchronizer>(new UnboundedChannelOptions()
        {
            SingleReader = true,
            SingleWriter = false
        });
    
    /// <summary>
    /// Метод для обработки узла
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <remarks><paramref name="token"/> может быть отменен, когда переходим в новое состояние</remarks>
    public async Task StartServingAsync(CancellationToken token)
    {
        await foreach (var requestSynchronizer in Queue.ReadAllRequestsAsync(token))
        {
            using var _ = OperationScope.Begin(this);
            var success = await ProcessRequestAsync(requestSynchronizer, token);
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
            var request = new AppendEntriesRequest(Node.CurrentTerm, Node.Log.CommitIndex, Node.Id,
                Node.Log.LastLogEntryInfo, Node.Log[Info.MatchIndex..synchronizer.LogEntryIndex]);

            var response = await Peer.SendAppendEntries(request, token);
            
            // 2. Если ответ не вернулся (null) - сделать еще одну попытку: goto 1
            if (response is null)
            {
                continue;
            }
            
            // 3. Если ответ успешный 
            if (response.Success)
            {
                // 3.1. Обновить nextIndex = + кол-во Entries в запросе
                // 3.2. Обновить matchIndex = новый nextIndex - 1
                Info.Update(request.Entries.Length);
                
                synchronizer.NotifyComplete();
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

    private INode Node =>
        State.Node;

    public void NotifyHeartbeatTimeout()
    {
        if (!IsBusy)
        {
            Queue.AddHeartbeat();
        }
    }
}