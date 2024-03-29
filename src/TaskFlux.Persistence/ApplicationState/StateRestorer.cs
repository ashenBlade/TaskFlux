using TaskFlux.Consensus;
using TaskFlux.Core.Restore;
using TaskFlux.Persistence.ApplicationState.Deltas;

namespace TaskFlux.Persistence.ApplicationState;

public static class StateRestorer
{
    /*
     * Шаги:
     * 1. При восстановлении состояния больше не будем использоваить бизнес-логику - только внутренние структуры
     * 2. Добавить метод для создания менеджера очередей из существующих очередей, точнее их описания (код, политики, ограничения и т.д.) вместе с содержимым
     *
     */
    public static QueueCollection RestoreState(ISnapshot? snapshot, IEnumerable<byte[]> deltas)
    {
        var collection = snapshot is not null
                             ? QueuesSnapshotSerializer.Deserialize(new SnapshotStream(snapshot))
                             : QueueCollection.CreateDefault();

        foreach (var delta in deltas.Select(Delta.DeserializeFrom))
        {
            delta.Apply(collection);
        }

        return collection;
    }
}