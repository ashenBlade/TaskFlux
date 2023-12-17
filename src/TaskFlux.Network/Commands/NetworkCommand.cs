using TaskFlux.Network.Exceptions;
using Utils.Serialization;

namespace TaskFlux.Network.Commands;

public abstract class NetworkCommand
{
    internal NetworkCommand()
    {
    }

    public abstract NetworkCommandType Type { get; }
    public abstract ValueTask SerializeAsync(Stream stream, CancellationToken token);
    public abstract T Accept<T>(INetworkCommandVisitor<T> visitor);

    public static async ValueTask<NetworkCommand> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var marker = await reader.ReadByteAsync(token);

        switch (( NetworkCommandType ) marker)
        {
            case NetworkCommandType.Enqueue:
                return await EnqueueNetworkCommand.DeserializeAsync(stream, token);
            case NetworkCommandType.Dequeue:
                return await DequeueNetworkCommand.DeserializeAsync(stream, token);
            case NetworkCommandType.Count:
                return await CountNetworkCommand.DeserializeAsync(stream, token);
            case NetworkCommandType.ListQueues:
                return new ListQueuesNetworkCommand();
            case NetworkCommandType.CreateQueue:
                return await CreateQueueNetworkCommand.DeserializeAsync(stream, token);
            case NetworkCommandType.DeleteQueue:
                return await DeleteQueueNetworkCommand.DeserializeAsync(stream, token);
        }

        throw new UnknownCommandTypeException(marker);
    }
}