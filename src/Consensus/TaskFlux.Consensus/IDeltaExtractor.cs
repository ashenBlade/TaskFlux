namespace TaskFlux.Consensus;

public interface IDeltaExtractor<in TResponse>
{
    /// <summary>
    /// Получить дельту изменений, представленную в виде массива байт
    /// </summary>
    /// <param name="response">Объект, из которого нужно получить дельту</param>
    /// <param name="delta">Массив байт дельты изменений</param>
    /// <returns><c>true</c> - дельта была получена, <c>false</c> - дельты нет</returns>
    public bool TryGetDelta(TResponse response, out byte[] delta);
}