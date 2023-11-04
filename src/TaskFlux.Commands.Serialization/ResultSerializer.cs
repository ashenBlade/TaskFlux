using System.Diagnostics;
using System.Runtime.Serialization;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.Visitors;
using TaskFlux.Serialization.Helpers;
using TaskQueue.Core.Exceptions;

namespace TaskFlux.Commands.Serialization;

/// <summary>
/// Сериализатор класса <see cref="Result"/> для сетевой передачи
/// </summary>
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
            Debug.Assert(result is not null,
                "result is not null",
                "ListQueuesResult при сериализации не должен быть null");

            _buffer = MetadataSerializerHelpers.SerializeMetadata(result.Metadata);
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
                   ResultType.Count      => DeserializeCountResult(ref reader),
                   ResultType.Dequeue    => DeserializeDequeueResult(ref reader),
                   ResultType.Enqueue    => DeserializeEnqueueResult(ref reader),
                   ResultType.Error      => DeserializeErrorResult(ref reader),
                   ResultType.Ok         => OkResult.Instance,
                   ResultType.ListQueues => DeserializeListQueuesResult(ref reader),
               };
    }

    private ListQueuesResult DeserializeListQueuesResult(ref ArrayBinaryReader reader)
    {
        var infos = MetadataSerializerHelpers.DeserializeMetadata(ref reader);
        return new ListQueuesResult(infos);
    }

    private static ErrorResult DeserializeErrorResult(ref ArrayBinaryReader reader)
    {
        var subtype = ( ErrorType ) reader.ReadByte();
        var message = reader.ReadString();
        return new ErrorResult(subtype, message);
    }

    private static CountResult DeserializeCountResult(ref ArrayBinaryReader reader)
    {
        var count = reader.ReadInt32();
        if (count == 0)
        {
            return CountResult.Empty;
        }

        return new CountResult(count);
    }

    private EnqueueResult DeserializeEnqueueResult(ref ArrayBinaryReader reader)
    {
        var success = reader.ReadBoolean();
        return success
                   ? EnqueueResult.Ok
                   : EnqueueResult.Full;
    }

    private DequeueResult DeserializeDequeueResult(ref ArrayBinaryReader reader)
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