namespace TaskFlux.Consensus.State.LeaderState;

/// <summary>
/// Запрос на синхронизацию логов и отправку узлам AppendEntries.
/// Когда узел его получает он должен найти новые записи в логе для текущего узла
/// и отправить запрос AppendEntries на узел.
/// Когда команда (с этим логом) отправлена на узел, то вызывается <see cref="NotifyComplete"/>,
/// которая инкрементирует счетчик успешных репликаций.
/// Как только кворум собран (высчитывается в <see cref="_peers"/>),
/// вызывающая сторона сигнализируется о завершении обработки (<see cref="_signal"/>) 
/// </summary>
public sealed class LogReplicationRequest : IDisposable
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
    public Lsn LogIndex { get; }

    /// <summary>
    /// Количество отданных успешных ответов/репликаций
    /// </summary>
    private volatile int _votes;

    private readonly PeerGroup _peers;
    private readonly ManualResetEvent _signal;
    private volatile bool _disposed;
    private Term? _greaterTerm;

    public LogReplicationRequest(
        PeerGroup peers,
        Lsn logIndex)
    {
        _signal = new ManualResetEvent(false);
        _peers = peers;
        LogIndex = logIndex;
    }

    /// <summary>
    /// Вызывается, когда на узел был реплицирован лог до требуемого индекса
    /// </summary>
    public void NotifyComplete()
    {
        if (_disposed)
        {
            return;
        }

        var incremented = Interlocked.Increment(ref _votes);
        // Если у нас кворум большинства, то 
        // для достижение консенсуса - тот момент,
        // когда для текущего числа голосов кворум достигнут, а для на один меньше уже нет
        if (Check(incremented) && !Check(incremented - 1))
        {
            try
            {
                _signal.Set();
            }
            catch (ObjectDisposedException)
            {
                // Ситуация говна, ладно
            }
        }

        bool Check(int count) => _peers.IsQuorumReached(count);
    }

    /// <summary>
    /// Вызывается, когда в ответе от узла обнаружился больший терм
    /// </summary>
    public void NotifyFoundGreaterTerm(Term greaterTerm)
    {
        if (_disposed)
        {
            return;
        }

        // Проверки на сравнение термов можно не делать -
        // даже если потом найдется больший терм - сигнал уже будет выставлен
        // и выполнение продолжится
        _greaterTerm = greaterTerm;

        try
        {
            _signal.Set();
        }
        catch (ObjectDisposedException)
        {
            // Все равно можем в такую ситуацию попасть
        }
    }

    /// <summary>
    /// Дождаться окончания репликации
    /// </summary>
    public void Wait(CancellationToken token)
    {
        try
        {
            WaitHandle.WaitAny(new[] { token.WaitHandle, _signal });
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
    }

    public bool TryGetGreaterTerm(out Term greaterTerm)
    {
        if (_greaterTerm is { } term)
        {
            greaterTerm = term;
            return true;
        }

        greaterTerm = Term.Start;
        return false;
    }

    public void Dispose()
    {
        _disposed = true;
        _signal.Dispose();
    }
}