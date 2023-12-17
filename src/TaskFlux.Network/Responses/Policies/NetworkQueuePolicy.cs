using Utils.Serialization;

namespace TaskFlux.Network.Responses.Policies;

public abstract class NetworkQueuePolicy
{
    public abstract NetworkPolicyCode Code { get; }
    public abstract Task Serialize(Stream stream, CancellationToken token);

    public static async Task<NetworkQueuePolicy> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var marker = await reader.ReadInt32Async(token);
        return ( NetworkPolicyCode ) marker switch
               {
                   NetworkPolicyCode.Generic => await GenericNetworkQueuePolicy.DeserializeAsync(stream, token),
                   NetworkPolicyCode.MaxQueueSize => await MaxQueueSizeNetworkQueuePolicy.DeserializeAsync(stream,
                                                         token),
                   NetworkPolicyCode.PriorityRange => await PriorityRangeNetworkQueuePolicy.DeserializeAsync(stream,
                                                          token),
                   NetworkPolicyCode.MaxMessageSize => await MaxMessageSizeNetworkQueuePolicy.DeserializeAsync(stream,
                                                           token),
               };
    }
}