using TaskFlux.PriorityQueue;

namespace TaskFlux.Core.Queue;

public interface ITaskQueueBuilder
{
    ITaskQueueBuilder WithQueueName(QueueName name);
    ITaskQueueBuilder WithData(IReadOnlyCollection<QueueRecord> data);
    ITaskQueueBuilder WithMaxQueueSize(int? maxSize);
    ITaskQueueBuilder WithPriorityRange(long min, long max);
    ITaskQueueBuilder WithQueueImplementation(PriorityQueueCode implementation);
    ITaskQueueBuilder WithMaxPayloadSize(int? maxPayloadSize);
    ITaskQueueBuilder WithLastRecordId(RecordId id);

    /// <summary>
    /// Создать очередь с указанными параметрами
    /// </summary>
    /// <returns>Созданная очередь</returns>
    /// <exception cref="InvalidOperationException">Указанный набор параметров представляет неправильную комбинацию</exception>
    ITaskQueue Build();
}