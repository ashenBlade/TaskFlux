namespace TaskFlux.Network.Packets.Responses;

public enum NetworkResponseType : byte
{
    Ok = ( byte ) 'k',
    Count = ( byte ) 'c',
    Error = ( byte ) 'e',

    Dequeue = ( byte ) 'd',
    ListQueues = ( byte ) 'l',
    PolicyViolation = ( byte ) 'p',
}