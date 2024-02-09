using TaskFlux.Consensus.Persistence;

namespace TaskFlux.Consensus.State.LeaderState;

/// <summary>
/// Объект для коммита записей, которые были реплицированы на большинство узлов.
/// Используется вместо того, чтобы вручную коммитить записи.
/// </summary>
internal class ReplicationWatcher
{
    /// <summary>
    /// Данные о состоянии репликации данных на узел
    /// </summary>
    private readonly PeerReplicationState[] _peerInfos;

    /// <summary>
    /// Хранилище состояния 
    /// </summary>
    private readonly IPersistence _persistence;

    /// <summary>
    /// Последний закоммиченный индекс.
    /// Используется это значение, вместо того, чтобы постоянно вызывать Commit()
    /// </summary>
    private Lsn _lastKnownCommitIndex;

    public ReplicationWatcher(PeerReplicationState[] peerInfos, IPersistence persistence)
    {
        _peerInfos = peerInfos;
        _persistence = persistence;
        _lastKnownCommitIndex = persistence.CommitIndex;
    }

    /// <summary>
    /// Уведомить о том, что какой-то индекс был реплицирован.
    /// В случае, если какой-то индекс был реплицирован на большинство узлов и не был закоммичен - выполняет коммит
    /// </summary>
    public void Notify()
    {
        /*
         * Для определения индекс записи, которая была закоммичена на большинстве узлов используется поиск КАК БЫ медианы.
         * Берется не медиана, а следующее после нее число, так как мы НЕЯВНО добавляем свой индекс, т.к. у нас эта запись тоже есть,
         * но это число самое большое из возможных, поэтому просто не добавляем.
         * В итоге, поиск этого числа производится 2 действиями:
         * 1. Сортируем все текущие индексы на каждом узле
         * 2. Получаем середину сортированного массива
         *
         * Полученное число - индекс нового коммита
         */
        lock (_peerInfos)
        {
            // Надеюсь, никто не захочет создавать кластер из 1024 * 1024 узлов, так что ок
            Span<long> nextIndexes = stackalloc long[_peerInfos.Length];

            for (var i = 0; i < _peerInfos.Length; i++)
            {
                nextIndexes[i] = ( long ) _peerInfos[i].NextIndex - 1;
            }

            nextIndexes.Sort();

            var mostReplicatedIndex = nextIndexes[nextIndexes.Length / 2];
            if (mostReplicatedIndex == Lsn.TombIndex || mostReplicatedIndex <= _lastKnownCommitIndex)
            {
                return;
            }

            _persistence.Commit(mostReplicatedIndex);
            _lastKnownCommitIndex = mostReplicatedIndex;
        }
    }
}