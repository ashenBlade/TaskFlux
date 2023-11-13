using System.Diagnostics;
using System.Runtime.Serialization;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.PolicyViolation;
using TaskFlux.Commands.Serialization.Exceptions;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Core.Policies;
using TaskFlux.Models.Exceptions;
using Utils.Serialization;

namespace TaskFlux.Commands.Serialization;

/// <summary>
/// Сериализатор класса <see cref="Response"/> для сетевой передачи
/// </summary>
public class ResponseSerializer
{
    public static readonly ResponseSerializer Instance = new();

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
                    ( byte ) ResponseType.Dequeue, // Маркер 
                    0                              // Успех: false
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

        public void Visit(PolicyViolationResponse response)
        {
            _buffer = response.ViolatedPolicy.Accept(QueuePolicySerializerVisitor.Instance);
        }
    }

    /// <summary>
    /// Десериализовать массив байт в объект результата выполнения запроса
    /// </summary>
    /// <param name="payload">Сериализованное представление <see cref="Response"/></param>
    /// <returns>
    /// Десериализованный <see cref="Response"/>
    /// </returns>
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
                   ResponseType.Count           => DeserializeCountResponse(ref reader),
                   ResponseType.Dequeue         => DeserializeDequeueResponse(ref reader),
                   ResponseType.Error           => DeserializeErrorResponse(ref reader),
                   ResponseType.Ok              => OkResponse.Instance,
                   ResponseType.ListQueues      => DeserializeListQueuesResponse(ref reader),
                   ResponseType.PolicyViolation => DeserializePolicyViolationResponse(ref reader),
               };
    }

    private PolicyViolationResponse DeserializePolicyViolationResponse(ref ArrayBinaryReader reader)
    {
        var code = reader.ReadInt32();
        var policy = DeserializeQueuePolicy(ref reader, code);
        return new PolicyViolationResponse(policy);

        static QueuePolicy DeserializeQueuePolicy(ref ArrayBinaryReader reader, int code)
        {
            return ( PolicyCode ) code switch
                   {
                       PolicyCode.MaxQueueSize   => DeserializeMaxQueueSizePolicy(ref reader),
                       PolicyCode.MaxPayloadSize => DeserializeMaxPayloadSizePolicy(ref reader),
                       PolicyCode.PriorityRange  => DeserializePriorityRangePolicy(ref reader),
                       _                         => throw new UnknownPolicyException(code),
                   };
        }

        static PriorityRangeQueuePolicy DeserializePriorityRangePolicy(ref ArrayBinaryReader reader)
        {
            var min = reader.ReadInt64();
            var max = reader.ReadInt64();
            return new PriorityRangeQueuePolicy(min, max);
        }

        static MaxPayloadSizeQueuePolicy DeserializeMaxPayloadSizePolicy(ref ArrayBinaryReader reader)
        {
            var maxPayloadSize = reader.ReadInt32();
            return new MaxPayloadSizeQueuePolicy(maxPayloadSize);
        }

        static MaxQueueSizeQueuePolicy DeserializeMaxQueueSizePolicy(ref ArrayBinaryReader reader)
        {
            var maxQueueSize = reader.ReadInt32();
            return new MaxQueueSizeQueuePolicy(maxQueueSize);
        }
    }

    private ListQueuesResponse DeserializeListQueuesResponse(ref ArrayBinaryReader reader)
    {
        var infos = MetadataSerializerHelpers.DeserializeMetadata(ref reader);
        return new ListQueuesResponse(infos);
    }

    private static ErrorResponse DeserializeErrorResponse(ref ArrayBinaryReader reader)
    {
        var subtype = ( ErrorType ) reader.ReadByte();
        var message = reader.ReadString();
        return new ErrorResponse(subtype, message);
    }

    private static CountResponse DeserializeCountResponse(ref ArrayBinaryReader reader)
    {
        var count = reader.ReadInt32();
        if (count == 0)
        {
            return CountResponse.Empty;
        }

        return new CountResponse(count);
    }

    private DequeueResponse DeserializeDequeueResponse(ref ArrayBinaryReader reader)
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