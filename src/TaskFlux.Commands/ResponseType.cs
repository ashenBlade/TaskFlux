namespace TaskFlux.Commands;

public enum ResponseType : byte
{
    // Общие ответы
    Ok = ( byte ) 'k',
    Error = ( byte ) 'x',
    PolicyViolation = ( byte ) 'p',

    // Ответы, специализированные под конкретные ситуации
    Dequeue = ( byte ) 'd',
    Count = ( byte ) 'c',
    ListQueues = ( byte ) 'l',
}