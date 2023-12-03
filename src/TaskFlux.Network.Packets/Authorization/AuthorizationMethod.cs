using TaskFlux.Network.Packets.Exceptions;
using Utils.Serialization;

namespace TaskFlux.Network.Packets.Authorization;

public abstract class AuthorizationMethod
{
    public abstract AuthorizationMethodType AuthorizationMethodType { get; }

    internal AuthorizationMethod()
    {
    }

    public abstract int EstimatePayloadSize();
    public abstract void Serialize(ref MemoryBinaryWriter writer);
    public abstract void Accept(IAuthorizationMethodVisitor methodVisitor);

    public abstract ValueTask AcceptAsync(IAsyncAuthorizationMethodVisitor methodVisitor,
                                          CancellationToken token = default);

    public static async Task<AuthorizationMethod> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var marker = await reader.ReadByteAsync(token);
        switch (( AuthorizationMethodType ) marker)
        {
            case AuthorizationMethodType.None:
                return new NoneAuthorizationMethod();
        }

        throw new UnknownAuthorizationMethodException(marker);
    }
}