using System.Buffers;

namespace TaskFlux.Network.Requests.Serialization;

/// <summary>
/// Структура представляющая полученный из пула массив
/// </summary>
/// <param name="Array">Массив из пула</param>
/// <param name="Length">Длина массива с полезной нагрузкой</param>
public readonly record struct PooledBuffer(byte[] Array, int Length, ArrayPool<byte> Pool) : IDisposable
{
    public void Dispose()
    {
        if (Array is not null && Pool is not null)
        {
            Pool.Return(Array);
        }
    }

    public Memory<byte> ToMemory() => Array.AsMemory(0, Length);
}