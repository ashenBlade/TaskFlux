namespace TaskFlux.Core.Commands;

public enum ResponseType : byte
{
    // Общие ответы
    Ok = ( byte ) 'k',
    Error = ( byte ) 'x',
    PolicyViolation = ( byte ) 'p',

    // Ответы, специализированные под конкретные ситуации
    Dequeue = ( byte ) 'd',
    Subscription = ( byte ) 's',
    Count = ( byte ) 'c',
    ListQueues = ( byte ) 'l',
    CreateQueue = ( byte ) 'q',
    DeleteQueue = ( byte ) 'r',
    Enqueue = ( byte ) 'e',
}