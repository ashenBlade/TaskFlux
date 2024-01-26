using TaskFlux.Network.Exceptions;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Responses.Policies;

public abstract class NetworkQueuePolicy
{
    public abstract NetworkPolicyCode Code { get; }
    public abstract Task Serialize(Stream stream, CancellationToken token);

    public static async Task<NetworkQueuePolicy> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var marker = await reader.ReadInt32Async(token);
        switch (( NetworkPolicyCode ) marker)
        {
            case NetworkPolicyCode.Generic:
                return await GenericNetworkQueuePolicy.DeserializeAsync(stream, token);
            case NetworkPolicyCode.MaxQueueSize:
                return await MaxQueueSizeNetworkQueuePolicy.DeserializeAsync(stream, token);
            case NetworkPolicyCode.PriorityRange:
                return await PriorityRangeNetworkQueuePolicy.DeserializeAsync(stream, token);
            case NetworkPolicyCode.MaxMessageSize:
                return await MaxMessageSizeNetworkQueuePolicy.DeserializeAsync(stream, token);
        }

        throw new UnknownQueuePolicyException(marker);
    }
}