namespace TaskFlux.Commands.Error;

public enum ErrorType: byte
{
    Unknown = 0,
    InvalidQueueName = 1,
    QueueDoesNotExist = 2,
    QueueAlreadyExists = 3,
}