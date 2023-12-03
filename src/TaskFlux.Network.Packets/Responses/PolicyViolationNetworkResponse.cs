using TaskFlux.Network.Packets.Responses.Policies;
using Utils.Serialization;

namespace TaskFlux.Network.Packets.Responses;

public class PolicyViolationNetworkResponse : NetworkResponse
{
    public override NetworkResponseType Type => NetworkResponseType.PolicyViolation;

    public NetworkQueuePolicy ViolatedNetworkQueuePolicy { get; }

    public PolicyViolationNetworkResponse(NetworkQueuePolicy violatedNetworkQueuePolicy)
    {
        ViolatedNetworkQueuePolicy = violatedNetworkQueuePolicy;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var writer = new StreamBinaryWriter(stream);
        await writer.WriteAsync(( byte ) NetworkResponseType.PolicyViolation, token);
        await ViolatedNetworkQueuePolicy.Serialize(stream, token);
    }

    public new static async Task<PolicyViolationNetworkResponse> DeserializeAsync(
        Stream stream,
        CancellationToken token)
    {
        var policy = await NetworkQueuePolicy.DeserializeAsync(stream, token);
        return new PolicyViolationNetworkResponse(policy);
    }

    public override T Accept<T>(INetworkResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}