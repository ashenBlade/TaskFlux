namespace TaskFlux.Persistence.ApplicationState;

internal readonly struct QueueRecord : IEquatable<QueueRecord>
{
    public long Key { get; } = 0;
    public byte[] Message { get; } = Array.Empty<byte>();

    public QueueRecord(long key, byte[] message)
    {
        Key = key;
        Message = message;
    }

    public bool Equals(QueueRecord other)
    {
        return Key == other.Key
            && Message.Length == other.Message.Length
            && Message.SequenceEqual(other.Message);
    }

    public override bool Equals(object? obj)
    {
        return obj is QueueRecord other && Equals(other);
    }

    public override int GetHashCode()
    {
        var result = ( int ) ( Message.Length + Key );
        foreach (int val in Message)
        {
            result = unchecked( result * 314159 + val );
        }

        return result;
    }
}