using Utils.Serialization;

namespace TaskFlux.Network.Responses.Policies;

public class MaxMessageSizeNetworkQueuePolicy : NetworkQueuePolicy
{
    public int MaxMessageSize { get; }
    public override NetworkPolicyCode Code => NetworkPolicyCode.MaxMessageSize;

    public MaxMessageSizeNetworkQueuePolicy(int maxMessageSize)
    {
        MaxMessageSize = maxMessageSize;
    }

    public override async Task Serialize(Stream stream, CancellationToken token)
    {
        var writer = new StreamBinaryWriter(stream);
        await writer.WriteAsync(( int ) Code, token);
        await writer.WriteAsync(MaxMessageSize, token);
    }

    public new static async Task<MaxMessageSizeNetworkQueuePolicy> DeserializeAsync(
        Stream stream,
        CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var size = await reader.ReadInt32Async(token);
        return new MaxMessageSizeNetworkQueuePolicy(size);
    }
}