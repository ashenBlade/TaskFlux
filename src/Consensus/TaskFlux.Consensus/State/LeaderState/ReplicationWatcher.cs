using TaskFlux.Consensus.Persistence;

namespace TaskFlux.Consensus.State.LeaderState;

internal class ReplicationWatcher
{
    /// <summary>
    /// Данные о состоянии репликации данных на узел
    /// </summary>
    private readonly PeerReplicationState[] _peerInfos;

    /// <summary>
    /// Хранилище состояния 
    /// </summary>
    private readonly FileSystemPersistenceFacade _persistence;

    public ReplicationWatcher(PeerReplicationState[] peerInfos, FileSystemPersistenceFacade persistence)
    {
        _peerInfos = peerInfos;
        _persistence = persistence;
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
            Span<int> nextIndexes = stackalloc int[_peerInfos.Length];

            for (var i = 0; i < _peerInfos.Length; i++)
            {
                nextIndexes[i] = _peerInfos[i].NextIndex - 1;
            }

            nextIndexes.Sort();

            var mostReplicatedIndex = nextIndexes[nextIndexes.Length / 2];
            if (mostReplicatedIndex == LogEntryInfo.TombIndex)
            {
                return;
            }

            _persistence.Commit(mostReplicatedIndex);
        }
    }
}