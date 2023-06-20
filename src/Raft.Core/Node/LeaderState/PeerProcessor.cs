using System.Threading.Channels;
using Raft.Core.Commands;
using Raft.Core.Commands.AppendEntries;

namespace Raft.Core.Node;


internal record PeerProcessor(LeaderState State, IPeer Peer, IRequestQueue Queue)
{
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
        await foreach (var synchronizer in Queue.ReadAllRequestsAsync(token))
        {
            using var _ = OperationScope.Begin(this);
            
            // Заходим на новый раунд
                
            // Получаем новые команды
            var newEntries = synchronizer;
                
            // Если записи в логе есть - отправляем AppendEntries, чтобы синхронизировать лог
            if (newEntries is {Entries.Count: >0} entries)
            {
                throw new NotImplementedException("Пока не могу синхронизировать лог");
            }
            // Иначе отправляем обычный Heartbeat
            else
            {
                var request = AppendEntriesRequest.Heartbeat(
                    State.Node.CurrentTerm,
                    State.Node.Log.CommitIndex,
                    State.Node.Id,
                    State.Node.Log.LastLogEntry);
                var response = await Peer.SendAppendEntries(request, token);
                newEntries.NotifyComplete();
                if (response is null or {Success: true})
                {
                    continue;
                }

                if (State.Node.CurrentTerm < response.Term)
                {
                    State.Node.CommandQueue.Enqueue(new MoveToFollowerStateCommand(response.Term, null, State,
                        State.Node));
                }
            }
        }
    }

    public void NotifyNewLogEntries(AppendEntriesRequestSynchronizer entries)
    {
        _channel.Writer.TryWrite(entries);
    }

    public void NotifyHeartbeatTimeout()
    {
        if (!IsBusy)
        {
            Queue.AddHeartbeat();
        }
    }
}