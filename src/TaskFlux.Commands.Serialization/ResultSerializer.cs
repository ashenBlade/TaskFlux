using System.Runtime.Serialization;
using JobQueue.Core;
using JobQueue.Core.Exceptions;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.Visitors;
using TaskFlux.Serialization.Helpers;

namespace TaskFlux.Commands.Serialization;

public class ResultSerializer
{
    public static readonly ResultSerializer Instance = new();

    /// <summary>
    /// Сериализовать <see cref="Result"/> для передачи по сети
    /// </summary>
    /// <param name="result">Результат выполнения запроса</param>
    /// <returns>Сериализованный <paramref name="result"/> в байты</returns>
    /// <exception cref="SerializationException">Ошибка во время сериализации объекта</exception>
    public byte[] Serialize(Result result)
    {
        var visitor = new SerializerResultVisitor();
        result.Accept(visitor);
        return visitor.Buffer;
    }

    // Сделать с возвращением результата (для оптимизации памяти)
    private class SerializerResultVisitor : IResultVisitor
    {
        private byte[]? _buffer;

        public byte[] Buffer =>
            _buffer ?? throw new ArgumentNullException(nameof(_buffer), "Сериализованное значениеу не выставлено");

        public void Visit(EnqueueResult result)
        {
            var estimatedSize = sizeof(ResultType)
                              + sizeof(bool);
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write(( byte ) ResultType.Enqueue);
            writer.Write(result.Success);
            _buffer = buffer;
        }

        public void Visit(DequeueResult result)
        {
            if (result.TryGetResult(out var key, out var payload))
            {
                var estimatedSize = sizeof(ResultType)     // Маркер
                                  + sizeof(bool)           // Успех
                                  + sizeof(long)           // Ключ
                                  + sizeof(int)            // Размер данных
                                  + result.Payload.Length; // Данные
                var buffer = new byte[estimatedSize];
                var writer = new MemoryBinaryWriter(buffer);
                writer.Write(( byte ) ResultType.Dequeue);
                writer.Write(true);
                writer.Write(key);
                writer.WriteBuffer(payload);
                _buffer = buffer;
            }
            else
            {
                _buffer = new byte[]
                {
                    ( byte ) ResultType.Dequeue, 0 // Успех: false
                };
            }
        }

        public void Visit(CountResult result)
        {
            var estimatedSize = sizeof(ResultType) // Маркер
                              + sizeof(int);       // Количество
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write(( byte ) ResultType.Count);
            writer.Write(result.Count);
            _buffer = buffer;
        }

        public void Visit(ErrorResult result)
        {
            var estimatedMessageSize = MemoryBinaryWriter.EstimateResultSize(result.Message);
            var estimatedSize = sizeof(ResultType)    // Маркер
                              + sizeof(ErrorType)     // Тип ошибки
                              + estimatedMessageSize; // Сообщение 

            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);

            writer.Write(( byte ) ResultType.Error);
            writer.Write(( byte ) result.ErrorType);
            writer.Write(result.Message);

            _buffer = buffer;
        }

        public void Visit(OkResult result)
        {
            var estimatedSize = sizeof(ResultType);
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);

            writer.Write(( byte ) ResultType.Ok);

            _buffer = buffer;
        }

        public void Visit(ListQueuesResult result)
        {
            var estimatedSize = EstimateSize();
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);

            writer.Write(( byte ) ResultType.ListQueues);
            writer.Write(result.Metadata.Count);

            foreach (var metadata in result.Metadata)
            {
                // Название очереди (ключ)
                writer.Write(metadata.QueueName);

                // Метаданные очереди (список)
                if (metadata.HasMaxSize)
                {
                    writer.Write(2); // Размер очереди + максимальный размер

                    writer.Write("count");
                    writer.Write(metadata.Count.ToString());

                    writer.Write("limit");
                    writer.Write(metadata.MaxSize.ToString());
                }
                else
                {
                    writer.Write(1); // Размер очереди

                    writer.Write("count");
                    writer.Write(metadata.Count.ToString());
                }
            }

            _buffer = buffer;

            int EstimateSize()
            {
                var size = sizeof(ResultType) // Маркер
                         + sizeof(int);       // Кол-во элементов в словаре

                // Проверяем словарь
                foreach (var metadata in result.Metadata)
                {
                    // Размер ключа (название очереди)
                    size += MemoryBinaryWriter.EstimateResultSize(metadata.QueueName);

                    // Кол-во элементов списка
                    size += sizeof(int);

                    // Сами данные
                    if (metadata.HasMaxSize)
                    {
                        size += MemoryBinaryWriter.EstimateResultSize("count");
                        size += MemoryBinaryWriter.EstimateResultSize(metadata.Count.ToString());

                        size += MemoryBinaryWriter.EstimateResultSize("limit");
                        size += MemoryBinaryWriter.EstimateResultSize(metadata.MaxSize.ToString());
                    }
                    else
                    {
                        size += MemoryBinaryWriter.EstimateResultSize("count");
                        size += MemoryBinaryWriter.EstimateResultSize(metadata.Count.ToString());
                    }
                }

                return size;
            }
        }
    }

    /// <summary>
    /// Десериализовать массив байт в объект результата выполнения запроса
    /// </summary>
    /// <param name="payload">Сериализованное представление <see cref="Result"/></param>
    /// <returns>Десериализованный <see cref="Result"/></returns>
    /// <exception cref="InvalidQueueNameException">В поле названия очереди было представлено неверное значение</exception>
    /// <exception cref="SerializationException">Ошибка десериализации</exception>
    /// <exception cref="ArgumentNullException"><paramref name="payload"/> - <c>null</c></exception>
    public Result Deserialize(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        if (payload.Length == 0)
        {
            throw new SerializationException("Переденный массив байт пуст");
        }

        var reader = new ArrayBinaryReader(payload);
        var marker = ( ResultType ) reader.ReadByte();
        return marker switch
               {
                   ResultType.Count      => DeserializeCountResult(reader),
                   ResultType.Dequeue    => DeserializeDequeueResult(reader),
                   ResultType.Enqueue    => DeserializeEnqueueResult(reader),
                   ResultType.Error      => DeserializeErrorResult(reader),
                   ResultType.Ok         => OkResult.Instance,
                   ResultType.ListQueues => DeserializeListQueuesResult(reader),
               };
    }

    private ListQueuesResult DeserializeListQueuesResult(ArrayBinaryReader reader)
    {
        // Размер словаря (кол-во элементов в нем)
        var metadataCount = reader.ReadInt32();

        if (metadataCount == 0)
        {
            // Такого быть не может, т.к. всегда как минимум 1 очередь (по умолчанию), 
            // но на всякий случай
            return new ListQueuesResult(Array.Empty<IJobQueueMetadata>());
        }

        var result = new List<IJobQueueMetadata>(metadataCount);

        for (int i = 0; i < metadataCount; i++)
        {
            var queueName = reader.ReadQueueName();
            var size = reader.ReadInt32();
            var builder = new PlainJobQueueMetadata.Builder();
            builder.WithQueueName(queueName);

            for (int j = 0; j < size; j++)
            {
                var attribute = reader.ReadString();
                var value = reader.ReadString();
                switch (attribute)
                {
                    case "count":
                        builder.WithCount(uint.Parse(value));
                        break;
                    case "limit":
                        builder.WithMaxSize(uint.Parse(value));
                        break;
                    default:
                        // Если попали сюда, то другая версия или типа того.
                        // В любом случае, если обязательные поля отсутствуют, то исключение будет дальше.
                        break;
                }
            }

            result.Add(builder.Build());
        }

        return new ListQueuesResult(result);
    }

    private static ErrorResult DeserializeErrorResult(ArrayBinaryReader reader)
    {
        var subtype = ( ErrorType ) reader.ReadByte();
        var message = reader.ReadString();
        return new ErrorResult(subtype, message);
    }

    private static CountResult DeserializeCountResult(ArrayBinaryReader reader)
    {
        var count = reader.ReadUInt32();
        if (count == 0)
        {
            return CountResult.Empty;
        }

        return new CountResult(count);
    }

    private EnqueueResult DeserializeEnqueueResult(ArrayBinaryReader reader)
    {
        var success = reader.ReadBoolean();
        return success
                   ? EnqueueResult.Ok
                   : EnqueueResult.Full;
    }

    private DequeueResult DeserializeDequeueResult(ArrayBinaryReader reader)
    {
        var hasValue = reader.ReadBoolean();
        if (!hasValue)
        {
            return DequeueResult.Empty;
        }

        var key = reader.ReadInt64();
        var payload = reader.ReadBuffer();
        return DequeueResult.Create(key, payload);
    }
}