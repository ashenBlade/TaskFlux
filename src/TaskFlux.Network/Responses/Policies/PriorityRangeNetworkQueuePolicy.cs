using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Responses.Policies;

public class PriorityRangeNetworkQueuePolicy : NetworkQueuePolicy
{
    public override NetworkPolicyCode Code => NetworkPolicyCode.PriorityRange;
    public long Min { get; }
    public long Max { get; }

    public PriorityRangeNetworkQueuePolicy(long min, long max)
    {
        Min = min;
        Max = max;
    }

    public override async Task Serialize(Stream stream, CancellationToken token)
    {
        var writer = new StreamBinaryWriter(stream);
        await writer.WriteAsync(( int ) Code, token);
        await writer.WriteAsync(Min, token);
        await writer.WriteAsync(Max, token);
    }

    public new static async Task<PriorityRangeNetworkQueuePolicy> DeserializeAsync(
        Stream stream,
        CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var min = await reader.ReadInt64Async(token);
        var max = await reader.ReadInt64Async(token);
        return new PriorityRangeNetworkQueuePolicy(min, max);
    }
}