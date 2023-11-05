namespace TaskFlux.Commands.Error;

public static class DefaultErrors
{
    public static readonly ErrorResponse InvalidQueueName =
        new(ErrorType.InvalidQueueName, "Неправильное название очереди");

    public static readonly ErrorResponse QueueDoesNotExist =
        new(ErrorType.QueueDoesNotExist, "Очередь не существует");

    public static readonly ErrorResponse QueueAlreadyExists =
        new(ErrorType.QueueAlreadyExists, "Очередь уже существует");
}