using System.Buffers;

namespace TaskFlux.Consensus.Cluster.Network;

internal readonly struct RentedBuffer : IDisposable
{
    private readonly ArrayPool<byte> _owner;
    public byte[]? Buffer { get; }
    public int Size { get; }

    public Memory<byte> GetMemory() =>
        Buffer?.AsMemory(0, Size) ?? throw new InvalidOperationException("Буфер данных не инициализирован");

    public Span<byte> GetSpan() =>
        Buffer != null
            ? Buffer.AsSpan(0, Size)
            : throw new InvalidOperationException("Буфер данных не инициализирован");

    public RentedBuffer(byte[] buffer, int size, ArrayPool<byte> owner)
    {
        _owner = owner;
        Buffer = buffer;
        Size = size;
    }

    public void Dispose()
    {
        if (Buffer is not null)
        {
            _owner.Return(Buffer);
        }
    }

    public static RentedBuffer Create(int size)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        return new RentedBuffer(buffer, size, ArrayPool<byte>.Shared);
    }
}