using System.Diagnostics;

namespace TaskFlux.PriorityQueue.Heap;

/// <summary>
/// Реализация приоритетной очереди, использующая 4-арную кучу - в каждом узле хранится 4 элемента, а не 1
/// </summary>
public class HeapPriorityQueue<TData> : IPriorityQueue<TData>
{
    /// <summary>
    /// Пороговое значение размера кучи, после которого мы будем проверять размер <see cref="_heap"/> для ее уменьшения при необходимости.
    /// Без такой оптимизации мы будем постоянно дергать GC, что уменьшит производительность.
    /// </summary>
    /// <remarks>
    /// 512 взял из головы.
    /// В будущем может сделаю бенчмарки, чтобы определить оптимальное значение, а пока я здесь хозяин-барин
    /// </remarks>
    private const int HeapSizeCheckThreshold = 512;

    /// <summary>
    /// Превышен ли порог размера кучи, после которого необходимо уменьшать ее размер
    /// </summary>
    private bool IsHeapSizeThresholdExceeded => _heap.Length < HeapSizeCheckThreshold;

    /// <summary>
    /// Приоритетная очередь, хранящаяся в виде массива кучи.
    /// Содержит только уникальные ключи.
    /// Для поддержки нескольких значений для одного и того же ключа, используется очередь значений.
    /// </summary>
    private HeapRecord<TData>[] _heap = Array.Empty<HeapRecord<TData>>();

    /// <summary>
    /// Размер занимаемой длины в массиве.
    /// </summary>
    /// <remarks>
    /// Текущее значение будет равняться индексом нового вставляемого элемента
    /// </remarks>
    private int _size;

    /// <summary>
    /// Отображение ключа (приоритета) на очередь, которая хранит записи
    /// </summary>
    private readonly Dictionary<long, ChunkedQueue<TData>> _keyToQueue = new();

    public PriorityQueueCode Code => PriorityQueueCode.Heap4Arity;
    public int Count { get; private set; }

    public void Enqueue(long key, TData payload)
    {
        if (_keyToQueue.TryGetValue(key, out var queue))
        {
            // Запись с нужным ключом уже существует - просто вставляем в существующую очередь
            queue.Enqueue(payload);

            // Обновляем количество элементов в очереди
            Count++;
            return;
        }

        // Записи с таким ключом еще нет 
        var record = new HeapRecord<TData>(key, payload);

        // Добавляем и в очередь,
        EnqueueCore(record);

        // И в кэш отображений
        _keyToQueue[key] = record.Queue;

        // Обновляем количество элементов в очереди
        Count++;
    }

    /// <summary>
    /// Вставить новый элемент в очередь
    /// </summary>
    /// <param name="record">Запись, которую нужно вставить</param>
    private void EnqueueCore(HeapRecord<TData> record)
    {
        /*
         * 1. Вставляем запись в последний лист дерева
         * 2. Пока ключ родителя меньше - обмениваем
         * 3. Если достигли конца (корень) - конец
         */

        if (_size == _heap.Length)
        {
            HeapGrow();
        }

        // Вставляем запись в конец кучи (последний лист)
        var index = _size++;
        _heap[index] = record;

        // Пока не достигнем корня
        while (0 < index)
        {
            // Вычисляем индекс родителя
            var parentIndex = GetParentIndex(index);

            // Если у родителя меньший ключ
            if (_heap[parentIndex].Key < record.Key)
            {
                // Прекращаем добавление, т.к. в куче родитель должен иметь меньший ключ
                break;
            }

            // Обновляем узлы и заходим на следующий круг
            SwapNodes(index, parentIndex);
            index = parentIndex;
        }
    }

    /// <summary>
    /// Поменять местами 2 узла
    /// </summary>
    /// <param name="first">Индекс одного узла</param>
    /// <param name="second">Индекс другого узла</param>
    private void SwapNodes(int first, int second)
    {
        ( _heap[first], _heap[second] ) = ( _heap[second], _heap[first] );
    }

    /// <summary>
    /// Рассчитать индекс родительского узла для переданного индекса потомка
    /// </summary>
    /// <param name="childIndex">Индекс потомка</param>
    /// <returns>Индекс родителя</returns>
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
        Debug.Assert(_size == _heap.Length, "_size == _records.Length",
            "Растить размер очереди нужно только, когда массив полностью заполнен. _size = {0}. _records.Length = {1}",
            _size, _heap.Length);

        if (_size == 0)
        {
            _heap = new HeapRecord<TData>[1];
            return;
        }

        // Сейчас в очереди находится как минимум 1 элемент.
        // Можно растить массив в 4 раза, чтобы поддерживать Capacity для полного 4-арного дерева,
        // но это будет неоптимально и однажды можем вырасти слишком сильно.
        // Рост в 2 раза каждый раз звучит логично.
        Array.Resize(ref _heap, _heap.Length * 2);
    }

    public bool TryDequeue(out long key, out TData payload)
    {
        // Если куча пуста
        if (_size == 0)
        {
            // То очевидно никаких данных в ней нет
            key = default;
            payload = default!;
            return false;
        }

        // Иначе, единственная возможная запись хранится в голове
        ( key, var queue ) = _heap[0];

        // Если в голове есть значения
        if (queue.TryDequeue(out var stored))
        {
            // То полученное значение - ответ
            payload = stored;

            // Если это была последняя запись
            if (queue.IsEmpty)
            {
                // Удаляем запись для ключа из кучи: из кэша по ключу 
                _keyToQueue.Remove(key);

                // И из самой кучи
                RemoveTop();

                // Позаботимся о занимаемом месте
                AdjustCapacity();
            }

            Count--;

            return true;
        }

        // Иначе, эта очередь была пуста и нужно очистить данные
        payload = default!;
        _heap = Array.Empty<HeapRecord<TData>>();
        return false;
    }

    internal (long Key, TData Payload) Dequeue()
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
            _heap = Array.Empty<HeapRecord<TData>>();
            _size = 0;
            return;
        }

        // Если в куче 2 элемента, то исход предрешен
        if (_size == 2)
        {
            _heap[0] = _heap[1];
            _size = 1;
            return;
        }

        /*
         * В остальных случаях запускаем обобщенный алгоритм:
         * 1. Меняем местами первый и последний элементы
         * 2. Поочердено сдвигаем верхушку вниз, пока:
         *    - Есть потомки
         *    - И их ключи меньше
         */

        // Получаем последний индекс из кучи и сразу уменьшаем размер
        var lastNodeIndex = --_size;

        // Ставим последний узел в корень всего дерева
        SwapNodes(0, lastNodeIndex);
        _heap[lastNodeIndex] = default!;

        var currentNodeIndex = 0;
        var currentKey = _heap[0].Key;

        int childIndex;
        while (( childIndex = GetFirstChildIndex(currentNodeIndex) ) < lastNodeIndex)
        {
            // Узел может быть не полностью заполнен
            var lastChildIndex = Math.Min(childIndex + 4, lastNodeIndex);

            // Находим узел с минимальным ключом (первый по умолчанию имеет минимальный ключ)
            var minKey = _heap[childIndex].Key;
            var minKeyChildIndex = childIndex;

            // Итерируемся по всем дочерним узлам
            while (++childIndex < lastChildIndex)
            {
                var currentChildKey = _heap[childIndex].Key;

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
            currentNodeIndex = minKeyChildIndex;
        }
    }


    /// <summary>
    /// Проверить наполненность массива с данными и уменьшить, если занимаемый размер меньше половины аллоцированного места
    /// </summary>
    private void AdjustCapacity()
    {
        // Если занимаемое место меньше половины
        if (IsHeapSizeThresholdExceeded
         && // Если первышено пороговое значение размера очереди  
            _size < _heap.Length / 2)
        {
            // То уменьшаем размер массива в 2 раза
            Array.Resize(ref _heap, _heap.Length / 2);
        }
    }

    /// <summary>
    /// Получить индекс первого потомка для указанного родителя
    /// </summary>
    /// <param name="parentIndex">Индекс родителя</param>
    /// <returns>Индекс потомка</returns>
    private static int GetFirstChildIndex(int parentIndex)
    {
        return ( parentIndex << 2 ) + 1;
    }

    public IReadOnlyCollection<(long Priority, TData Data)> ReadAllData()
    {
        if (_size == 0)
        {
            return Array.Empty<(long, TData )>();
        }

        var result = new List<(long, TData)>();

        for (var i = 0; i < _size; i++)
        {
            var (key, queue) = _heap[i];
            queue.CopyTo(key, result);
        }

        return result;
    }

    public IEnumerable<(long, TData)> ReadAllDataTest()
    {
        return _heap.SelectMany(x => x.Queue
                                      .GetAllRecordsTest()
                                      .Select(y => ( x.Key, y )));
    }

    public List<(long, TData)> DequeueAllTest()
    {
        return DequeueEnumerable().ToList();

        IEnumerable<(long, TData)> DequeueEnumerable()
        {
            while (TryDequeue(out var key, out var value))
            {
                yield return ( key, value );
            }
        }
    }
}