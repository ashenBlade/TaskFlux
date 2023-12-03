namespace TaskFlux.Network.Packets.Commands;

public enum NetworkCommandType : byte
{
    Enqueue = ( byte ) 'E',
    Dequeue = ( byte ) 'D',
    Count = ( byte ) 'C',
    CreateQueue = ( byte ) 'Q',
    DeleteQueue = ( byte ) 'R',
    ListQueues = ( byte ) 'L',
}