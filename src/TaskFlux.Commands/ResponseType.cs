namespace TaskFlux.Commands;

public enum ResponseType : byte
{
    Ok = ( byte ) 'k',
    Error = ( byte ) 'x',
    PolicyViolation = ( byte ) 'p',

    Dequeue = ( byte ) 'd',
    Count = ( byte ) 'c',
    ListQueues = ( byte ) 'l',
}