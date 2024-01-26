using System.Diagnostics;

namespace TaskFlux.Consensus;

public class SnapshotStream : Stream
{
    private readonly ISnapshot _snapshot;

    /// <summary>
    /// Перечисление всех чанков из снапшота.
    /// Если не null, значит открыт
    /// </summary>
    private IEnumerator<ReadOnlyMemory<byte>>? _chunkEnumerator;

    /// <summary>
    /// Очередной чанк данных, который мы осматриваем
    /// </summary>
    private (ReadOnlyMemory<byte> Chunk, int Position)? _chunkData;

    /// <summary>
    /// Был ли достигнут конец снапшота
    /// </summary>
    private bool _end;

    public SnapshotStream(ISnapshot snapshot)
    {
        _snapshot = snapshot;
    }

    public override void Flush()
    {
        throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return Read(buffer.AsSpan(offset, count));
    }

    public override int Read(Span<byte> buffer)
    {
        if (_end)
        {
            return 0;
        }

        if (_chunkEnumerator is null)
        {
            // Если еще ни разу не начинали чтение, то инициализируем перечислитель
            var enumerable = _snapshot.GetAllChunks();
            _chunkEnumerator = enumerable.GetEnumerator();
        }


        if (!TryGetChunk(out var chunk, out var position))
        {
            // Больше чанков нет - снапшот закончился
            return 0;
        }

        // Копируем столько, сколько вообще можем
        var chunkLeft = chunk.Length - position;
        var copyCount = Math.Min(buffer.Length, chunkLeft);
        chunk.Span.Slice(position, copyCount).CopyTo(buffer);

        // Сдвигаем позицию на скопированное число байт
        // и обновляем данные по текущему чанку
        position += copyCount;

        if (chunk.Length <= position)
        {
            // В случае, если чанк закончился, то выставляем чанк в null
            // прочитаем следующий в следующем вызове Read
            _chunkData = null;
        }
        else
        {
            // Чанк еще не закончился, оставшееся прочитаем на следующем вызове
            _chunkData = ( chunk, position );
        }

        return copyCount;
    }

    /// <summary>
    /// Получить очередной чанк данных из снапшота, вернуть его и позицию, с которой нужно его читать
    /// </summary>
    /// <param name="chunk"></param>
    /// <param name="position"></param>
    /// <returns></returns>
    private bool TryGetChunk(out ReadOnlyMemory<byte> chunk, out int position)
    {
        Debug.Assert(_chunkEnumerator is not null, "_chunkEnumerator is not null",
            "Перечислитель чанка должен быть инициализирован на момент вызова");

        // Чтение предыдущего чанка еще не закончено
        if (_chunkData is var (ch, pos))
        {
            chunk = ch;
            position = pos;
            return true;
        }

        // Иначе читаем весь снапшот, пока не дойдем до непустого нового чанка
        do
        {
            if (!_chunkEnumerator.MoveNext())
            {
                // Достигли конца чанка, работа завершена
                _end = true;

                chunk = ReadOnlyMemory<byte>.Empty;
                position = 0;
                return false;
            }

            chunk = _chunkEnumerator.Current;
            position = 0;
        } while (chunk.Length == 0);

        return true;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _chunkEnumerator?.Dispose();
            _chunkData = null;
        }

        base.Dispose(disposing);
    }
}