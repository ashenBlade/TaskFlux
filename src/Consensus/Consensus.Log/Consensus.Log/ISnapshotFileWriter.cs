using Consensus.Core.Log;

namespace Consensus.Log;

/// <summary>
/// Интерфейс для записи файла снапшота
/// </summary>
/// <remarks>
/// Представляет собой некоторый вариант <see cref="IDisposable"/>, только вместо <see cref="IDisposable.Dispose"/>,
/// операции: <see cref="Save"/> и <see cref="Discard"/>.
/// Они обязательно должны быть вызваны для очищения ресурсов.1
/// </remarks>
public interface ISnapshotFileWriter
{
    /// <summary>
    /// Записать в файл заголовок снапшота
    /// </summary>
    /// <param name="lastApplied">Информация о последней применной записи снапшота</param>
    /// <exception cref="InvalidOperationException">Методы был вызван повторно, либо уже были вызваны <see cref="Save"/> или <see cref="Discard"/></exception>
    public void Initialize(LogEntryInfo lastApplied);

    /// <summary>
    /// Сохранить полученный файл в качестве нового файла снапшота
    /// </summary>
    /// <remarks>Сохранить сделанные записи в файл снапшота</remarks>
    /// <exception cref="InvalidOperationException">Ничего не записано, либо уже были вызваны <see cref="Save"/> или <see cref="Discard"/></exception>
    /// <exception cref="ApplicationException">Обнаружена попытка одновременного обновления файла снапшота</exception>
    public void Save();

    /// <summary>
    /// Отменить сделанные операции - не сохранять записанный снапшот.
    /// </summary>
    /// <remarks>
    /// Вызов метода идемпотентен - можно вызывать в любом состоянии любое количество раз без исключения <see cref="InvalidOperationException"/>
    /// </remarks>
    /// <remarks>Вызывается в случае отмены записи. Например, когда разрывается связь или новый лидер начинает новую операцию</remarks>
    public void Discard();

    /// <summary>
    /// Записать данные снапшота в файл
    /// </summary>
    /// <param name="snapshot">Объект снапшота</param>
    /// <param name="token">Токен отмены</param>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    public void WriteSnapshot(ISnapshot snapshot, CancellationToken token);
}