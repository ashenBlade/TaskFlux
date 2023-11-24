namespace Consensus.Raft;

public interface ICommandSerializer<in TCommand>
{
    /// <summary>
    /// Получить дельту изменений, представленную в виде массива байт
    /// </summary>
    /// <param name="command">Команда, из которой нужно получить дельту</param>
    /// <param name="delta">Массив байт дельты изменений</param>
    /// <returns><c>true</c> - дельта была получена, <c>false</c> - дельты нет</returns>
    public bool TryGetDelta(TCommand command, out byte[] delta);
}