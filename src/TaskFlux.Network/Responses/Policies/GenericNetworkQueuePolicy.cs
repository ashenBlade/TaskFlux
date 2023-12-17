using Utils.Serialization;

namespace TaskFlux.Network.Responses.Policies;

public class GenericNetworkQueuePolicy : NetworkQueuePolicy
{
    public override NetworkPolicyCode Code => NetworkPolicyCode.Generic;

    public string Message { get; }

    public GenericNetworkQueuePolicy(string message)
    {
        Message = message;
    }

    public override async Task Serialize(Stream stream, CancellationToken token)
    {
        var writer = new StreamBinaryWriter(stream);
        await writer.WriteAsync(( int ) Code, token);
        await writer.WriteAsync(Message, token);
    }

    public new static async Task<GenericNetworkQueuePolicy> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var message = await reader.ReadStringAsync(token);
        return new GenericNetworkQueuePolicy(message);
    }
}