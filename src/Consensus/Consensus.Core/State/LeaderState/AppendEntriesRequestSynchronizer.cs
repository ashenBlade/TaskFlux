namespace Consensus.Core.State.LeaderState;

/// <summary>
/// Запрос на синхронизацию логов и отправку узлам AppendEntries.
/// Когда узел его получает он должен найти новые записи в логе для текущего узла
/// (информация о последних обработанных записях хранится в <see cref="PeerInfo"/>)
/// и отправить запрос AppendEntries на узел.
/// Когда команда (с этим логом) отправлена на узел, то вызывается <see cref="NotifyComplete"/>,
/// которая инкрементирует счетчик успешных репликаций.
/// Как только кворум собран (высчитывается в <see cref="_quorumChecker"/>) то <see cref="_tcs"/> завершается,
/// а вызывающая сторона сигнализируется о завершении обработки (таска завершена) 
/// </summary>
internal class AppendEntriesRequestSynchronizer
{
    /// <summary>
    /// Индекс записи в логе, которая вызвала событие.
    /// Когда запись под этим индексом успешно отправлена,
    /// то работа закончена
    /// </summary>
    /// <remarks>
    /// В процессе работы в лог могут добавиться несколько команд.
    /// Чтобы точно знать какие команды необходимо посылать (когда остановиться),
    /// используется этот индекс
    /// </remarks>
    public int LogEntryIndex { get; }
    
    private volatile int _votes = 0;
    private readonly IQuorumChecker _quorumChecker;
    private readonly TaskCompletionSource _tcs = new();
    
    public AppendEntriesRequestSynchronizer(
        IQuorumChecker quorumChecker,
        int logEntryIndex)
    {
        _quorumChecker = quorumChecker;
        LogEntryIndex = logEntryIndex;
    }

    public Task LogReplicated => _tcs.Task;

    public void NotifyComplete()
    {
        var incremented = Interlocked.Increment(ref _votes);
        
        if (Check(incremented) && 
            !Check(incremented - 1))
        {
            try
            {
                _tcs.SetResult();
            }
            catch (InvalidOperationException)
            { }
        }

        bool Check(int count) => _quorumChecker.IsQuorumReached(count);
    }
}