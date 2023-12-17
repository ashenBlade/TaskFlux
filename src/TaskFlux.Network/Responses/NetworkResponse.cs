using TaskFlux.Network.Exceptions;
using Utils.Serialization;

namespace TaskFlux.Network.Responses;

public abstract class NetworkResponse
{
    internal NetworkResponse()
    {
    }

    public abstract NetworkResponseType Type { get; }
    public abstract ValueTask SerializeAsync(Stream stream, CancellationToken token);
    public abstract T Accept<T>(INetworkResponseVisitor<T> visitor);

    public static async ValueTask<NetworkResponse> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var marker = await reader.ReadByteAsync(token);
        switch (( NetworkResponseType ) marker)
        {
            case NetworkResponseType.Count:
                return await CountNetworkResponse.DeserializeAsync(stream, token);
            case NetworkResponseType.Error:
                return await ErrorNetworkResponse.DeserializeAsync(stream, token);
            case NetworkResponseType.Dequeue:
                return await DequeueNetworkResponse.DeserializeAsync(stream, token);
            case NetworkResponseType.ListQueues:
                return await ListQueuesNetworkResponse.DeserializeAsync(stream, token);
            case NetworkResponseType.PolicyViolation:
                return await PolicyViolationNetworkResponse.DeserializeAsync(stream, token);
        }

        throw new UnknownResponseTypeException(marker);
    }
}