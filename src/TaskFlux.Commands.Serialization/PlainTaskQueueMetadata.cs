using System.Runtime.Serialization;
using TaskQueue.Core;

namespace TaskFlux.Commands.Serialization;

internal class PlainTaskQueueMetadata : ITaskQueueMetadata
{
    private PlainTaskQueueMetadata(QueueName queueName, uint count, uint maxSize)
    {
        QueueName = queueName;
        Count = count;
        MaxSize = maxSize;
    }

    public QueueName QueueName { get; }
    public uint Count { get; }
    public uint MaxSize { get; }

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

        private uint? _count;

        public uint Count =>
            _count ?? throw new SerializationException("В переданных метаданных не было размера очереди");

        public Builder WithCount(uint count)
        {
            _count = count;
            return this;
        }

        private uint? _maxSize;
        public uint MaxSize => _maxSize ?? 0;

        public Builder WithMaxSize(uint maxSize)
        {
            _maxSize = maxSize;
            return this;
        }

        public ITaskQueueMetadata Build()
        {
            return new PlainTaskQueueMetadata(QueueName, Count, MaxSize);
        }
    }
}