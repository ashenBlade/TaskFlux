namespace TaskFlux.Core.Commands.Error;

/// <summary>
/// Ошибка бизнес-логики, возникающая во время выполнения команды
/// </summary>
public enum ErrorType : int
{
    /// <summary>
    /// Неизвестный тип ошибки
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// Указана неправльное название очереди
    /// </summary>
    InvalidQueueName = 1,

    /// <summary>
    /// Указанная очередь не существует. Возникает при попытке доступа к несуществующей очереди
    /// </summary>
    QueueDoesNotExist = 2,

    /// <summary>
    /// Очередь с указанным названием уже существует. Возникает при попытку создания новой очереди
    /// </summary>
    QueueAlreadyExists = 3,

    /// <summary>
    /// Указано некорректное значение диапазона приоритетов
    /// </summary>
    InvalidPriorityRange = 4,

    /// <summary>
    /// Указано некорректное значение максимального размера очереди
    /// </summary>
    InvalidMaxQueueSize = 5,

    /// <summary>
    /// Указано некорректное значение максимального размера сообщения
    /// </summary>
    InvalidMaxPayloadSize = 6,

    /// <summary>
    /// Диапазон приоритетов не указан
    /// </summary>
    PriorityRangeNotSpecified = 7,

    /// <summary>
    /// Получен неизвестный код приоритетной очереди
    /// </summary>
    UnknownPriorityQueueCode = 8,
}