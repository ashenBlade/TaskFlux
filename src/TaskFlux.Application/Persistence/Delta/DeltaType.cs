namespace TaskFlux.Application.Persistence.Delta;

/// <summary>
/// Тип дельты из файла лога
/// </summary>
public enum DeltaType : byte
{
    /// <summary>
    /// Создать очередь
    /// </summary>
    CreateQueue = ( byte ) 'C',

    /// <summary>
    /// Удалить очередь
    /// </summary>
    DeleteQueue = ( byte ) 'D',

    /// <summary>
    /// Добавить запись в очередь
    /// </summary>
    AddRecord = ( byte ) 'A',

    /// <summary>
    /// Удалить запись из очереди
    /// </summary>
    RemoveRecord = ( byte ) 'R',
}