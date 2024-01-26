namespace TaskFlux.Core.Commands.Error;

public static class DefaultErrors
{
    public static readonly ErrorResponse InvalidQueueName =
        new(ErrorType.InvalidQueueName, string.Empty);

    public static readonly ErrorResponse QueueDoesNotExist =
        new(ErrorType.QueueDoesNotExist, string.Empty);

    public static readonly ErrorResponse QueueAlreadyExists =
        new(ErrorType.QueueAlreadyExists, string.Empty);
}