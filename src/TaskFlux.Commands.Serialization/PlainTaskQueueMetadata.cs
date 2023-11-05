using System.Runtime.Serialization;
using TaskQueue.Core;
using TaskQueue.Models;

namespace TaskFlux.Commands.Serialization;

internal class PlainTaskQueueMetadata : ITaskQueueMetadata
{
    private PlainTaskQueueMetadata(QueueName queueName,
                                   int count,
                                   int? maxSize,
                                   (long, long)? priorityRange,
                                   int? maxPayloadSize)
    {
        QueueName = queueName;
        Count = count;
        MaxSize = maxSize;
        MaxPayloadSize = maxPayloadSize;
        PriorityRange = priorityRange;
    }

    public QueueName QueueName { get; }
    public int Count { get; }
    public int? MaxSize { get; }
    public int? MaxPayloadSize { get; }
    public (long Min, long Max)? PriorityRange { get; }

    public struct Builder
    {
        private QueueName? _queueName;

        public QueueName QueueName =>
            _queueName ?? throw new SerializationException("В переданных метаданных не было названия очереди");

        public Builder WithQueueName(QueueName name)
        {
            _queueName = name;
            return this;
        }

        private int? _count;

        public int Count =>
            _count ?? throw new SerializationException("В переданных метаданных не было размера очереди");

        public Builder WithCount(int count)
        {
            _count = count;
            return this;
        }

        private int? _maxSize;

        public Builder WithMaxSize(int maxSize)
        {
            _maxSize = maxSize;
            return this;
        }

        private (long, long)? _priority;

        public Builder WithPriorityRange((long, long) priority)
        {
            _priority = priority;
            return this;
        }

        private int? _maxPayloadSize;

        public Builder WithMaxPayloadSize(int maxPayloadSize)
        {
            _maxPayloadSize = maxPayloadSize;
            return this;
        }

        public ITaskQueueMetadata Build()
        {
            return new PlainTaskQueueMetadata(QueueName, Count, _maxSize, _priority, _maxPayloadSize);
        }
    }
}