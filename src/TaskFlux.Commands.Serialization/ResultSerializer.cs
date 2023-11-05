using System.Diagnostics;
using System.Runtime.Serialization;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.Visitors;
using TaskQueue.Core.Exceptions;
using Utils.Serialization;

namespace TaskFlux.Commands.Serialization;

/// <summary>
/// Сериализатор класса <see cref="Response"/> для сетевой передачи
/// </summary>
public class ResultSerializer
{
    public static readonly ResultSerializer Instance = new();

    /// <summary>
    /// Сериализовать <see cref="Response"/> для передачи по сети
    /// </summary>
    /// <param name="response">Результат выполнения запроса</param>
    /// <returns>Сериализованный <paramref name="response"/> в байты</returns>
    /// <exception cref="SerializationException">Ошибка во время сериализации объекта</exception>
    public byte[] Serialize(Response response)
    {
        var visitor = new SerializerResponseVisitor();
        response.Accept(visitor);
        return visitor.Buffer;
    }

    // Сделать с возвращением результата (для оптимизации памяти)
    private class SerializerResponseVisitor : IResponseVisitor
    {
        private byte[]? _buffer;

        public byte[] Buffer =>
            _buffer ?? throw new ArgumentNullException(nameof(_buffer), "Сериализованное значениеу не выставлено");

        public void Visit(EnqueueResponse response)
        {
            var estimatedSize = sizeof(ResponseType)
                              + sizeof(bool);
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write(( byte ) ResponseType.Enqueue);
            writer.Write(response.Success);
            _buffer = buffer;
        }

        public void Visit(DequeueResponse response)
        {
            if (response.TryGetResult(out var key, out var payload))
            {
                var estimatedSize = sizeof(ResponseType)     // Маркер
                                  + sizeof(bool)             // Успех
                                  + sizeof(long)             // Ключ
                                  + sizeof(int)              // Размер данных
                                  + response.Payload.Length; // Данные
                var buffer = new byte[estimatedSize];
                var writer = new MemoryBinaryWriter(buffer);
                writer.Write(( byte ) ResponseType.Dequeue);
                writer.Write(true);
                writer.Write(key);
                writer.WriteBuffer(payload);
                _buffer = buffer;
            }
            else
            {
                _buffer = new byte[]
                {
                    ( byte ) ResponseType.Dequeue, 0 // Успех: false
                };
            }
        }

        public void Visit(CountResponse response)
        {
            var estimatedSize = sizeof(ResponseType) // Маркер
                              + sizeof(int);         // Количество
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write(( byte ) ResponseType.Count);
            writer.Write(response.Count);
            _buffer = buffer;
        }

        public void Visit(ErrorResponse response)
        {
            var estimatedMessageSize = MemoryBinaryWriter.EstimateResultSize(response.Message);
            var estimatedSize = sizeof(ResponseType)  // Маркер
                              + sizeof(ErrorType)     // Тип ошибки
                              + estimatedMessageSize; // Сообщение 

            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);

            writer.Write(( byte ) ResponseType.Error);
            writer.Write(( byte ) response.ErrorType);
            writer.Write(response.Message);

            _buffer = buffer;
        }

        public void Visit(OkResponse response)
        {
            var estimatedSize = sizeof(ResponseType);
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);

            writer.Write(( byte ) ResponseType.Ok);

            _buffer = buffer;
        }

        public void Visit(ListQueuesResponse response)
        {
            Debug.Assert(response is not null,
                "result is not null",
                "ListQueuesResult при сериализации не должен быть null");

            _buffer = MetadataSerializerHelpers.SerializeMetadata(response.Metadata);
        }
    }

    /// <summary>
    /// Десериализовать массив байт в объект результата выполнения запроса
    /// </summary>
    /// <param name="payload">Сериализованное представление <see cref="Response"/></param>
    /// <returns>Десериализованный <see cref="Response"/></returns>
    /// <exception cref="InvalidQueueNameException">В поле названия очереди было представлено неверное значение</exception>
    /// <exception cref="SerializationException">Ошибка десериализации</exception>
    /// <exception cref="ArgumentNullException"><paramref name="payload"/> - <c>null</c></exception>
    public Response Deserialize(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        if (payload.Length == 0)
        {
            throw new SerializationException("Переденный массив байт пуст");
        }

        var reader = new ArrayBinaryReader(payload);
        var marker = ( ResponseType ) reader.ReadByte();
        return marker switch
               {
                   ResponseType.Count      => DeserializeCountResult(ref reader),
                   ResponseType.Dequeue    => DeserializeDequeueResult(ref reader),
                   ResponseType.Enqueue    => DeserializeEnqueueResult(ref reader),
                   ResponseType.Error      => DeserializeErrorResult(ref reader),
                   ResponseType.Ok         => OkResponse.Instance,
                   ResponseType.ListQueues => DeserializeListQueuesResult(ref reader),
               };
    }

    private ListQueuesResponse DeserializeListQueuesResult(ref ArrayBinaryReader reader)
    {
        var infos = MetadataSerializerHelpers.DeserializeMetadata(ref reader);
        return new ListQueuesResponse(infos);
    }

    private static ErrorResponse DeserializeErrorResult(ref ArrayBinaryReader reader)
    {
        var subtype = ( ErrorType ) reader.ReadByte();
        var message = reader.ReadString();
        return new ErrorResponse(subtype, message);
    }

    private static CountResponse DeserializeCountResult(ref ArrayBinaryReader reader)
    {
        var count = reader.ReadInt32();
        if (count == 0)
        {
            return CountResponse.Empty;
        }

        return new CountResponse(count);
    }

    private EnqueueResponse DeserializeEnqueueResult(ref ArrayBinaryReader reader)
    {
        var success = reader.ReadBoolean();
        return success
                   ? EnqueueResponse.Ok
                   : EnqueueResponse.Full;
    }

    private DequeueResponse DeserializeDequeueResult(ref ArrayBinaryReader reader)
    {
        var hasValue = reader.ReadBoolean();
        if (!hasValue)
        {
            return DequeueResponse.Empty;
        }

        var key = reader.ReadInt64();
        var payload = reader.ReadBuffer();
        return DequeueResponse.Create(key, payload);
    }
}