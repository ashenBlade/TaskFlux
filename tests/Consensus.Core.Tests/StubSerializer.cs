namespace Consensus.Core.Tests;


public class StubSerializer<T> : ISerializer<T>
{
    public static readonly StubSerializer<T> Default =
        new() {Deserialized = default, Serialized = Array.Empty<byte>()};
    
    public byte[]? Serialized { get; set; }
    public T? Deserialized { get; set; }
        
    public byte[] Serialize(T command)
    {
        return Serialized ?? throw new Exception("Сериализованное значение не выставлено");
    }

    public T Deserialize(byte[] payload)
    {
        return Deserialized ?? throw new Exception("Десериализованное значение не выставлено");
    }
}