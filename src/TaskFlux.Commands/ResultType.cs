namespace TaskFlux.Commands;

public enum ResultType: byte
{
    Enqueue = (byte) 'e',
    Dequeue = (byte) 'd',
    Count = (byte) 'c',
    // TODO: добавить это в доку
    Error = (byte) 'E'
}