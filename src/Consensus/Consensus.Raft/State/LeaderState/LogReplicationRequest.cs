namespace Consensus.Raft.State.LeaderState;

/// <summary>
/// Запрос на синхронизацию логов и отправку узлам AppendEntries.
/// Когда узел его получает он должен найти новые записи в логе для текущего узла
/// (информация о последних обработанных записях хранится в <see cref="PeerInfo"/>)
/// и отправить запрос AppendEntries на узел.
/// Когда команда (с этим логом) отправлена на узел, то вызывается <see cref="NotifyComplete"/>,
/// которая инкрементирует счетчик успешных репликаций.
/// Как только кворум собран (высчитывается в <see cref="_peers"/>) то <see cref="_tcs"/> завершается,
/// а вызывающая сторона сигнализируется о завершении обработки (таска завершена) 
/// </summary>
public class LogReplicationRequest : IDisposable
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
    public int LogIndex { get; }

    private volatile int _votes = 0;
    private readonly PeerGroup _peers;

    private readonly ManualResetEventSlim _resetEventSlim;

    public LogReplicationRequest(
        PeerGroup peers,
        int logIndex)
    {
        _resetEventSlim = new ManualResetEventSlim(false);
        _peers = peers;
        LogIndex = logIndex;
    }

    public void NotifyComplete()
    {
        var incremented = Interlocked.Increment(ref _votes);
        if (Check(incremented) && !Check(incremented - 1))
        {
            try
            {
                _resetEventSlim.Set();
            }
            catch (InvalidOperationException)
            {
            }
        }

        bool Check(int count) => _peers.IsQuorumReached(count);
    }

    /// <summary>
    /// Дождаться окончания репликации
    /// </summary>
    public void Wait(CancellationToken token = default)
    {
        try
        {
            _resetEventSlim.Wait(token);
        }
        catch (OperationCanceledException)
        {
        }
    }

    public void Dispose()
    {
        _resetEventSlim.Dispose();
    }
}