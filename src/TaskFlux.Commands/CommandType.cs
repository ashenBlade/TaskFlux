namespace TaskFlux.Commands;

public enum CommandType: byte
{
    Enqueue = ( byte ) 'E',
    Dequeue = ( byte ) 'D',
    Count = ( byte ) 'C',
    CreateQueue = (byte) 'Q',
    DeleteQueue = (byte) 'R',
    ListQueues = (byte) 'L',
}