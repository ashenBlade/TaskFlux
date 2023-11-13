namespace TaskFlux.Commands.Error;

/// <summary>
/// Ошибка бизнес-логики, возникающая во время выполнения команды
/// </summary>
public enum ErrorType : byte
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
    /// При создании очереди был указан неверный набор параметров
    /// </summary>
    InvalidQueueParameters = 4,
}