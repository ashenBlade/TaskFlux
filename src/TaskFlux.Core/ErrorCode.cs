namespace TaskQueue.Core;

/// <summary>
/// Ошибка, возникшая при выполнении команды
/// </summary>
public enum ErrorCode
{
    QueueDoesNotExist = 0,
    QueueAlreadyExists = 1,
}