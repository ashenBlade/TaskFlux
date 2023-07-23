using Consensus.Core;
using TaskFlux.Commands;
using TaskFlux.Commands.Serialization;

namespace TaskFlux.Host.Infrastructure;

public class ProxyCommandSerializer: ISerializer<Command>
{
    private readonly CommandSerializer _serializer = new();
    public byte[] Serialize(Command command) => _serializer.Serialize(command);
    public Command Deserialize(byte[] payload) => _serializer.Deserialize(payload);
}