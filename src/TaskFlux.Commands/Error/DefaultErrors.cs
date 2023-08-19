namespace TaskFlux.Commands.Error;

public static class DefaultErrors
{
    public static readonly ErrorResult InvalidQueueName =
        new(ErrorType.InvalidQueueName, "Неправильное название очереди");

    public static readonly ErrorResult QueueDoesNotExist = 
        new(ErrorType.QueueDoesNotExist, "Очередь не существует");

    public static readonly ErrorResult QueueAlreadyExists =
        new(ErrorType.QueueAlreadyExists, "Очередь уже существует");
}