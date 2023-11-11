using System.Diagnostics;

namespace TaskFlux.PriorityQueue.Heap;

/// <summary>
/// Реализация приоритетной очереди, использующая 4-арную кучу - в каждом узле хранится 4 элемента, а не 1
/// </summary>
public class HeapPriorityQueue : IPriorityQueue
{
    /// <summary>
    /// Приоритетная очередь, хранящаяся в виде массива кучи.
    /// Содержит только уникальные ключи.
    /// Для поддержки нескольких значений для одного и того же ключа, используется очередь значений.
    /// </summary>
    private HeapRecord[] _records = Array.Empty<HeapRecord>();

    /// <summary>
    /// Размер занимаемой длины в массиве.
    /// </summary>
    /// <remarks>
    /// Текущее значение будет равняться индексом нового вставляемого элемента
    /// </remarks>
    private int _size = 0;

    /// <summary>
    /// Отображение ключа (приоритета) на очередь, которая хранит записи
    /// </summary>
    private readonly Dictionary<long, ChunkedQueue> _keyToQueue = new();

    public int Count => _records.Sum(hr => hr.Queue.Count);

    public void Enqueue(long key, byte[] payload)
    {
        if (_keyToQueue.TryGetValue(key, out var queue))
        {
            // Запись с нужным ключом уже существует - просто вставляем в существующую очередь
            queue.Enqueue(payload);
            return;
        }

        // Записи с таким ключом еще нет 
        var record = new HeapRecord(key, payload);

        // Добавляем и в очередь,
        EnqueueCore(record);

        // И в кэш отображений
        _keyToQueue[key] = record.Queue;
    }

    /// <summary>
    /// Вставить новый элемент в очередь
    /// </summary>
    /// <param name="record">Запись, которую нужно вставить</param>
    private void EnqueueCore(HeapRecord record)
    {
        /*
         * 1. Вставляем запись в последний лист дерева
         * 2. Пока ключ родителя меньше - обмениваем
         * 3. Если достигли конца (корень) - конец
         */
        if (_size == _records.Length)
        {
            HeapGrow();
        }

        var index = _size++;
        _records[index] = record;

        while (index != 0) // Пока не достигнем корня
        {
            // Вычисляем индекс родителя
            var parentIndex = GetParentIndex(index);

            if (record.Key > _records[parentIndex].Key)
            {
                // Родитель имеет меньший ключ (более приоритетный),
                // То дошли до конца
                break;
            }

            // Обмениваем значения
            SwapNodes(index, parentIndex);

            // Обновляем текущий индекс
            index = parentIndex;
        }
    }

    private void SwapNodes(int index, int parentIndex)
    {
        ( _records[index], _records[parentIndex] ) = ( _records[parentIndex], _records[index] );
    }

    private int GetParentIndex(int childIndex)
    {
        /*
         * Получение индекса родителя по индексу потомка в k-арной куче (0-based indexing):
         *  (index - 1) / k
         *
         * Для 4-арной = (index - 1) / 4
         * Деление на 4 и смещение на 2 бита вправо - одно и то же.
         *
         * Пруфы:
         *                          0
         *          /        /          \            \
         *         1         2           3            4
         *      / | | \   / |  | \    / | | \     / |  | \
         *     5  6 7  8 9 10 11 12 13 14 15 16 17 18 19 20
         *
         * 0 = (1 - 1) / 4
         * 0 = (2 - 1) / 4
         * 3 = (14 - 1) / 4
         * 4 = (20 - 1) / 4
         */

        return ( childIndex - 1 ) >> 2; // (childIndex - 1) / 4
    }

    /// <summary>
    /// Увеличить размер очереди на нужное количество элементов
    /// </summary>
    private void HeapGrow()
    {
        Debug.Assert(_size == _records.Length, "_size == _records.Length",
            "Растить размер очереди нужно только, когда массив полностью заполнен. _size = {0}. _records.Length = {1}",
            _size, _records.Length);

        if (_size == 0)
        {
            _records = new HeapRecord[1];
            return;
        }

        // Сейчас в очереди находится как минимум 1 элемент.
        // Можно растить массив в 4 раза, чтобы поддерживать Capacity для полного 4-арного дерева,
        // но это будет неоптимально и однажды можем вырасти слишком сильно.
        // Рост в 2 раза каждый раз звучит логично.
        Array.Resize(ref _records, _records.Length * 2);
    }

    public bool TryDequeue(out long key, out byte[] payload)
    {
        // Если куча пуста
        if (_size == 0)
        {
            // То очевидно никаких данных в ней нет
            key = default;
            payload = Array.Empty<byte>();
            return false;
        }

        // Иначе, единственная возможная запись хранится в голове
        ( key, var queue ) = _records[0];

        // Если в голове есть значения
        if (queue.TryDequeue(out var stored))
        {
            // То полученное значение - ответ
            payload = stored;

            // Если это была последняя запись
            if (queue.Count == 0)
            {
                // Удаляем запись для ключа из кучи: из кэша по ключу 
                _keyToQueue.Remove(key);

                // И из самой кучи
                RemoveTop();
            }

            return true;
        }

        // Иначе, эта очередь была пуста и нужно очистить данные
        payload = Array.Empty<byte>();
        _records = Array.Empty<HeapRecord>();
        return false;
    }

    internal (long Key, byte[] Payload) Dequeue()
    {
        if (TryDequeue(out var key, out var payload))
        {
            return ( key, payload );
        }

        throw new InvalidOperationException("Очередь пуста");
    }

    /// <summary>
    /// Удалить корень дерева.
    /// Вызывается, когда был вызыван <see cref="TryDequeue"/> и в корне оставался последний элемент (он теперь пуст).
    /// </summary>
    private void RemoveTop()
    {
        Debug.Assert(_size > 0, "_size > 0", "Очередь пуста, нельзя удалить корень");

        /*
         * 1. Находим потомка с наименьшим ключом
         * 2. Ставим этого потомка на наше место
         * 3. Переходим к следующему потомку (которого вставили)
         * 4. Если дошли до листа - конец
         */

        // Если в куче был только 1 элемент, то куча может только стать пустой
        if (_size == 1)
        {
            _records = Array.Empty<HeapRecord>();
            _size = 0;
            return;
        }

        // Если в куче 2 элемента, то исход предрешен
        if (_size == 2)
        {
            _records[0] = _records[1];
            _size = 1;
            return;
        }

        /*
         * В остальных случаях запускаем обобщенный алгоритм:
         * 1. Меняем местами первый и последний элементы
         * 2. Поочердено сдвигаем верхушку вниз, пока:
         *    - Есть потомки
         *    - И у их ключи меньше
         */

        var lastNodeIndex = _size - 1;
        SwapNodes(0, lastNodeIndex);

        var currentNodeIndex = 0;
        var currentKey = _records[0].Key;
        do
        {
            // Получаем диапазон индексов допустимых потомков
            var childIndex = GetFirstChildIndex(currentNodeIndex);

            // Узел может быть не полностью заполнен
            var lastChildIndex = Math.Min(childIndex + 4, _size - 1);

            // Находим узел с минимальным ключом (первый по умолчанию имеет минимальный ключ)
            var minKey = _records[childIndex].Key;
            var minKeyChildIndex = childIndex;

            // Итерируемся по всем узлам
            while (++childIndex < lastChildIndex)
            {
                var currentChildKey = _records[childIndex].Key;

                // Если ключ очередного узла меньше минимального
                if (currentChildKey < minKey)
                {
                    // Обновляем данные о текущем минимуме
                    minKeyChildIndex = childIndex;
                    minKey = currentChildKey;
                }
            }

            // Если среди потомков нет с меньшим ключом
            if (currentKey < minKey)
            {
                // То прекращаем работу
                break;
            }

            // Иначе обмениваем узлы и идем дальше вниз
            SwapNodes(currentNodeIndex, minKeyChildIndex);
            currentKey = minKey;
            currentNodeIndex = minKeyChildIndex;
        } while (currentNodeIndex < _size);

        // После всего уменьшаем размер кучи (тот что вставили - это и есть последний, так что норм)
        _size--;
    }

    /// <summary>
    /// Получить индекс первого потомка для указанного родителя
    /// </summary>
    /// <param name="parentIndex">Индекс родителя</param>
    /// <returns>Индекс потомка</returns>
    private static int GetFirstChildIndex(int parentIndex)
    {
        return ( parentIndex >> 2 ) + 1;
    }

    public IReadOnlyCollection<(long Priority, byte[] Payload)> ReadAllData()
    {
        if (_size == 0)
        {
            return Array.Empty<(long, byte[])>();
        }

        var result = new List<(long, byte[])>();

        for (var i = 0; i < _size; i++)
        {
            var (key, queue) = _records[i];
            queue.CopyTo(key, result);
        }

        return result;
    }

    public IEnumerable<(long, byte[])> ReadAllDataTest()
    {
        return _records.SelectMany(x => x.Queue
                                         .GetAllRecordsTest()
                                         .Select(y => ( x.Key, y )));
    }

    public List<(long, byte[])> DequeueAllTest()
    {
        return DequeueEnumerable().ToList();

        IEnumerable<(long, byte[])> DequeueEnumerable()
        {
            while (TryDequeue(out var key, out var value))
            {
                yield return ( key, value );
            }
        }
    }
}