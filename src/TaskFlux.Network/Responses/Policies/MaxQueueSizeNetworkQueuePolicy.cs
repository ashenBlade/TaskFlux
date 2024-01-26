using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Responses.Policies;

public class MaxQueueSizeNetworkQueuePolicy : NetworkQueuePolicy
{
    public int MaxQueueSize { get; }
    public override NetworkPolicyCode Code => NetworkPolicyCode.MaxQueueSize;

    public MaxQueueSizeNetworkQueuePolicy(int maxQueueSize)
    {
        MaxQueueSize = maxQueueSize;
    }

    public override async Task Serialize(Stream stream, CancellationToken token)
    {
        var writer = new StreamBinaryWriter(stream);
        await writer.WriteAsync(( int ) Code, token);
        await writer.WriteAsync(MaxQueueSize, token);
    }

    public new static async Task<MaxQueueSizeNetworkQueuePolicy> DeserializeAsync(
        Stream stream,
        CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var size = await reader.ReadInt32Async(token);
        return new MaxQueueSizeNetworkQueuePolicy(size);
    }
}