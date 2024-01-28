using System.Diagnostics;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Core;

namespace TaskFlux.Consensus.State.LeaderState;

/// <summary>
/// Информация об узле, необходимая для взаимодействия с ним в состоянии <see cref="NodeRole.Leader"/>
/// </summary>
internal class PeerReplicationState
{
    /// <summary>
    /// Индекс следующей записи в логе, которую необходимо отправить клиенту
    /// </summary>
    public Lsn NextIndex { get; private set; }

    public PeerReplicationState(Lsn nextIndex)
    {
        NextIndex = nextIndex;
    }

    /// <summary>
    /// Добавить к последнему индексу указанное число.
    /// Используется, когда запись (или несколько) были успешно реплицированы - не отправка снапшота
    /// </summary>
    /// <param name="appliedCount">Количество успешно отправленных записей</param>
    public void Increment(int appliedCount)
    {
        var nextIndex = NextIndex + appliedCount;
        Debug.Assert(0 <= nextIndex, "0 <= nextIndex",
            "Выставленный индекс следующей записи не может получиться отрицательным. Рассчитано: {0}. Кол-во примененных записей: {1}",
            nextIndex, appliedCount);
        NextIndex = nextIndex;
    }

    /// <summary>
    /// Выставить нужное число 
    /// </summary>
    /// <param name="nextIndex">Новый следующий индекс</param>
    public void Set(Lsn nextIndex)
    {
        Debug.Assert(0 <= nextIndex, "Следующий индекс записи не может быть отрицательным",
            "Нельзя выставлять отрицательный индекс следующей записи. Попытка выставить {0}. Старый следующий индекс: {1}",
            nextIndex, NextIndex);
        NextIndex = nextIndex;
    }

    /// <summary>
    /// Откатиться назад, если узел ответил на AppendEntries <c>false</c>
    /// </summary>
    /// <exception cref="InvalidOperationException"><see cref="NextIndex"/> равен 0</exception>
    public void Decrement()
    {
        if (NextIndex.Value is 0)
        {
            throw new InvalidOperationException("Нельзя откатиться на индекс меньше 0");
        }

        NextIndex--;
    }

    public override string ToString() => $"PeerInfo(NextIndex = {NextIndex})";
}