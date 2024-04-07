using System.Collections;
using TaskFlux.Core.Queue;
using TaskFlux.PriorityQueue;
using InvalidOperationException = System.InvalidOperationException;

namespace TaskFlux.Core.Restore;

/// <summary>
/// Объект, представляющий информацию об очереди.
/// По факту - ее слепок, который необходим при сериализации 
/// </summary>
public class QueueInfo
{
    public QueueInfo(QueueName queueName,
                     PriorityQueueCode code,
                     RecordId lastId,
                     int? maxQueueSize,
                     int? maxPayloadSize,
                     (long, long)? priorityRange)
    {
        QueueName = queueName;
        Code = code;
        LastId = lastId;
        MaxQueueSize = maxQueueSize;
        MaxPayloadSize = maxPayloadSize;
        PriorityRange = priorityRange;
        Data = new QueueRecordsValuesCollection(this);
    }

    public QueueInfo(QueueName queueName,
                     PriorityQueueCode code,
                     RecordId lastId,
                     int? maxQueueSize,
                     int? maxPayloadSize,
                     (long, long)? priorityRange,
                     IReadOnlyCollection<QueueRecord> records)
    {
        QueueName = queueName;
        Code = code;
        MaxQueueSize = maxQueueSize;
        MaxPayloadSize = maxPayloadSize;
        PriorityRange = priorityRange;
        Data = new QueueRecordsValuesCollection(this);
        ( LastId, _idToRecord ) = BuildRecords(records, lastId);
    }

    private static (RecordId LastId, Dictionary<RecordId, QueueRecordData> Records) BuildRecords(
        IReadOnlyCollection<QueueRecord> records,
        RecordId lastId)
    {
        var dict = new Dictionary<RecordId, QueueRecordData>(records.Count);
        foreach (var record in records)
        {
            dict.Add(record.Id, record.GetData());
            // Проверка на всякий случай
            if (lastId < record.Id)
            {
                lastId = record.Id;
            }
        }

        return ( lastId, dict );
    }

    /// <summary>
    /// Последняя запись назначенная очередью
    /// </summary>
    public RecordId LastId { get; private set; }

    /// <summary>
    /// Название очереди
    /// </summary>
    public QueueName QueueName { get; }

    /// <summary>
    /// Код реализации очереди
    /// </summary>
    public PriorityQueueCode Code { get; }

    /// <summary>
    /// Максимальный размер очереди, если есть
    /// </summary>
    public int? MaxQueueSize { get; }

    /// <summary>
    /// Максимальный размер нагрузки, если есть
    /// </summary>
    public int? MaxPayloadSize { get; }

    /// <summary>
    /// Допустимый диапазон приоритетов, если есть
    /// </summary>
    public (long, long)? PriorityRange { get; }

    /// <summary>
    /// Записи, которые хранятся в очереди
    /// </summary>
    /// <remarks>
    /// Данные хранятся в неупорядоченном виде
    /// </remarks>
    public IReadOnlyCollection<QueueRecord> Data { get; }

    /// <summary>
    /// Отображение ключа на множество его записей, вместо хранения абсолютно всех записей
    /// </summary>
    private readonly Dictionary<RecordId, QueueRecordData> _idToRecord = new();

    public void Add(long priority, byte[] payload)
    {
        var recordId = LastId.Increment();
        try
        {
            _idToRecord.Add(recordId, new QueueRecordData(priority, payload));
            LastId = recordId;
        }
        catch (ArgumentException ae)
        {
            throw new InvalidOperationException($"Запись с ID {recordId} уже присутствует в очереди", ae);
        }
    }

    public void Remove(RecordId id)
    {
        if (!_idToRecord.Remove(id))
        {
            throw new InvalidOperationException($"Записи с ID {id} не найдено");
        }
    }

    private class QueueRecordsValuesCollection : IReadOnlyCollection<QueueRecord>
    {
        private readonly QueueInfo _queueInfo;

        public QueueRecordsValuesCollection(QueueInfo queueInfo)
        {
            _queueInfo = queueInfo;
        }

        public IEnumerator<QueueRecord> GetEnumerator()
        {
            foreach (var (id, data) in _queueInfo._idToRecord)
            {
                yield return new QueueRecord(id, data.Priority, data.Payload);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count => _queueInfo._idToRecord.Count;
    }

    /// <summary>
    /// Метод для выставления готовых данных в очередь.
    /// </summary>
    /// <param name="records">Записи, которые нужно записать</param>
    /// <remarks>Используется только для тестов!</remarks>
    internal void SetDataTest(IEnumerable<QueueRecord> records)
    {
        _idToRecord.Clear();
        foreach (var record in records)
        {
            _idToRecord[record.Id] = record.GetData();
        }
    }
}