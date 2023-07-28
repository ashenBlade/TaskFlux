using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Serialization.Helpers;

namespace TaskFlux.Commands.Serialization;

public class CommandSerializer
{
    public static readonly CommandSerializer Instance = new();
    public byte[] Serialize(Command command)
    {
        var visitor = new CommandSerializerVisitor();
        command.Accept(visitor);
        return visitor.Result;
    }

    private class CommandSerializerVisitor : ICommandVisitor
    {
        private byte[]? _result;
        public byte[] Result => _result ?? throw new ArgumentNullException(nameof(_result), "Сериализованное значениеу не выставлено");
        
        public void Visit(EnqueueCommand command)
        {
            var estimatedSize = sizeof(CommandType)     // Маркер
                              + sizeof(int)             // Ключ
                              + sizeof(int)             // Длина тела
                              + command.Payload.Length; // Тело
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write((byte)CommandType.Enqueue);
            writer.Write(command.Key);
            writer.WriteBuffer(command.Payload);
            _result = buffer;
        }

        public void Visit(DequeueCommand command)
        {
            var estimatedSize = sizeof(CommandType);
            var buffer = new byte[estimatedSize];
            buffer[0] = ( byte ) CommandType.Dequeue;
            _result = buffer;
        }

        public void Visit(CountCommand command)
        {
            var estimatedSize = sizeof(CommandType);
            var buffer = new byte[estimatedSize];
            buffer[0] = ( byte ) CommandType.Count;
            _result = buffer;
        }
    }
    
    public Command Deserialize(byte[] payload)
    {
        var reader = new ArrayBinaryReader(payload);
        var marker = (CommandType) reader.ReadByte();
        return marker switch
               {
                   CommandType.Count   => CountCommand.Instance,
                   CommandType.Dequeue => DequeueCommand.Instance,
                   CommandType.Enqueue => DeserializeEnqueueCommand(reader),
               };
    }

    private EnqueueCommand DeserializeEnqueueCommand(ArrayBinaryReader reader)
    {
        var key = reader.ReadInt32();
        var buffer = reader.ReadBuffer();
        return new EnqueueCommand(key, buffer);
    }
}