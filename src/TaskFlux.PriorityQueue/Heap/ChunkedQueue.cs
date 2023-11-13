namespace TaskFlux.PriorityQueue.Heap;

/// <summary>
/// Очередь, реализованная в виде связного списка подчанков.
/// Каждый подчанк является условным массивом
/// </summary>
internal class ChunkedQueue
{
    /// <summary>
    /// Максимальный размер чанка, который можно достигнуть без создания следующего подчанка
    /// </summary>
    private const int MaxChunkSize = 256;

    /// <summary>
    /// Сама очередь в виде пары из головы и хвоста подчанков.
    /// <c>null</c> - очередь пуста.
    /// Голова и Хвост могут указывать на один и тот же объект - это значит очередь состоит из 1 чанка.
    /// </summary>
    private (QueueChunk Head, QueueChunk Tail)? _queue;

    /// <summary>
    /// Пуста ли очередь 
    /// </summary>
    /// <remarks>
    /// <c>true</c> - очередь пуста (элементов в ней нет), <c>false</c> - не пуста
    /// </remarks>
    public bool IsEmpty =>
        // _queue = null, только когда очередь пуста
        !_queue.HasValue;

    public void Enqueue(byte[] record)
    {
        // Если очередь уже инициализирована
        if (_queue is var (head, tail))
        {
            // Если размер хвоста дошел до максимального
            if (MaxChunkSize <= tail.Chunk.Count)
            {
                // Создаем новый хвост
                var newTail = new QueueChunk();

                // Добавляем запись в него
                newTail.Chunk.Enqueue(record);

                // Обновляем хвост
                tail.Next = newTail;
                _queue = ( head, newTail );
            }
            // Иначе очередь нормального размера
            else
            {
                // Добавляем в уже существующий хвост
                tail.Chunk.Enqueue(record);
            }
        }
        // Иначе, если она пуста
        else
        {
            // Создаем единственный чанк
            head = new QueueChunk();

            // Добавляем в него запись
            head.Chunk.Enqueue(record);

            // Обновляем состояние (изнчально голова и хвост одни и те же)
            _queue = ( head, head );
        }
    }

    public bool TryDequeue(out byte[] payload)
    {
        // Если очередь не пуста
        if (_queue is var (head, tail))
        {
            // Флаг необходимости обновления головы списка
            var shouldUpdate = false;

            while (head is not null)
            {
                // И голова не пуста (АААХАХАХАХАХАХХХАА)
                if (head.Chunk.TryDequeue(out var record))
                {
                    // То возвращаем первую запись из головы
                    payload = record;

                    // И если голова стала пуста
                    if (head.Chunk.Count == 0)
                    {
                        // То обновляем чанки

                        // Если очередь состояла из 1 чанка
                        if (head.Next is null)
                        {
                            // То зануляем всю очередь (нечего указатели хранить)
                            _queue = null;
                        }
                        // Иначе было несколько чанков
                        else
                        {
                            // Тогда головой становится следующий чанк.
                            // Дальше будет либо полностью заполненный чанк, либо частично - пустых быть не может, т.к. создание новых - ленивое
                            head = head.Next;
                            _queue = ( head, tail );
                        }
                    }

                    // Возвращаем полученный результат
                    if (shouldUpdate)
                    {
                        _queue = ( head, tail );
                    }

                    return true;
                }

                // Иначе, если дальше нет чанков
                if (head.Next is null)
                {
                    // Очередь стала полностью пуста
                    _queue = null;
                    payload = default!;
                    return false;
                }

                // Голова пуста и дальше есть еще чанки

                // Помечаем очередь для обновления в дальнейшем
                shouldUpdate = true;

                // Переходим к следующему чанку
                head = head.Next;
            }
        }

        // Иначе очередь была пуста и вернуть ничего не можем
        payload = default!;
        return false;
    }

    internal IEnumerable<byte[]> GetAllRecordsTest()
    {
        if (_queue is var (head, _))
        {
            var current = head;
            while (current is not null)
            {
                foreach (var data in current.Chunk)
                {
                    yield return data;
                }

                current = current.Next;
            }
        }
    }

    private class QueueChunk
    {
        /// <summary>
        /// Указатель на следующий чанк
        /// </summary>
        public QueueChunk? Next { get; set; }

        /// <summary>
        /// Очередь этого чанка с данными
        /// </summary>
        /// <remarks>
        /// Используется <see cref="Queue{T}"/>, т.к. внутри реализация использует массив
        /// </remarks>
        public Queue<byte[]> Chunk { get; } = new();
    }

    public void CopyTo(long key, List<(long, byte[])> result)
    {
        if (_queue is ({Chunk.Count: > 0} head, _))
        {
            var current = head;
            while (current is not null)
            {
                result.AddRange(current.Chunk.Select(x => ( key, x )));
                current = current.Next;
            }
        }
    }
}