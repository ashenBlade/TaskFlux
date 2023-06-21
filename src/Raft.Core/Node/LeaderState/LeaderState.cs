using System.Runtime.CompilerServices;
using Raft.Core.Commands;
using Raft.Core.Commands.Submit;
using Raft.Core.Log;
using Serilog;

namespace Raft.Core.Node.LeaderState;

internal class LeaderState: BaseNodeState
{
    public override NodeRole Role => NodeRole.Leader;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts = new();
    private readonly PeerProcessor[] _processors;

    internal LeaderState(INode node, ILogger logger, IRequestQueueFactory queueFactory)
        : base(node)
    {
        _logger = logger;
        _processors = node.PeerGroup
                          .Peers
                          .Select(x => new PeerProcessor(this, x, queueFactory.CreateQueue()))
                          .ToArray();
        
        Node.JobQueue.EnqueueInfinite(ProcessPeersAsync, _cts.Token);
        Node.HeartbeatTimer.Timeout += OnHeartbeatTimer;
    }
    
    private void OnHeartbeatTimer()
    {
        _logger.Verbose("Сработал Heartbeat таймер. Отправляю команду всем обработчикам узлов");
        Array.ForEach(_processors, static p => p.NotifyHeartbeatTimeout());
        Node.CommandQueue.Enqueue(new StartHeartbeatTimerCommand(this, Node));
    }

    private async Task ProcessPeersAsync()
    {
        _logger.Verbose("Запускаю обработчиков узлов");
        var tasks = _processors.Select(x => x.StartServingAsync(_cts.Token));
        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException)
            when (_cts.Token.IsCancellationRequested)
        { }
    }
    

    public override void Dispose()
    {
        Node.CommandQueue.Enqueue(new StopHeartbeatTimerCommand(this, Node));
        base.Dispose();
    }

    public static LeaderState Create(INode node)
    {
        return new LeaderState(node, node.Logger.ForContext("SourceContext", "Leader"), new ChannelRequestQueueFactory(node.Log));
    }

    public override SubmitResponse Apply(SubmitRequest request)
    {
        // Добавляем команду в лог
        var appended = Log.Append(new LogEntry( Node.CurrentTerm, request.Command ));
        
        // Сигнализируем узлам, чтобы принялись за работу
        var synchronizer = new AppendEntriesRequestSynchronizer(Node.PeerGroup, appended.Index);
        Array.ForEach(_processors, p => p.NotifyAppendEntries(synchronizer));
        
        // Ждем достижения кворума
        synchronizer.LogReplicated.Wait(_cts.Token);
        
        // Пытаемся применить команду к машине состояний
        Node.StateMachine.Submit(request.Command);
        
        // Обновляем индекс последней закоммиченной записи
        Log.Commit(appended.Index);
        
        // Возвращаем результат
        return new SubmitResponse(new LogEntry(appended.Term, request.Command));
    }
}