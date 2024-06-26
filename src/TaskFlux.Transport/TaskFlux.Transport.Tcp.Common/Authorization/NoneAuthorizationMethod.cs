using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Authorization;

public class NoneAuthorizationMethod : AuthorizationMethod
{
    public override AuthorizationMethodType AuthorizationMethodType => AuthorizationMethodType.None;

    public override int EstimatePayloadSize()
    {
        return sizeof(AuthorizationMethodType); // Только маркер один
    }

    public override void Serialize(ref MemoryBinaryWriter writer)
    {
        writer.Write((byte)AuthorizationMethodType.None);
    }

    public override void Accept(IAuthorizationMethodVisitor methodVisitor)
    {
        methodVisitor.Visit(this);
    }

    public override T Accept<T>(IAuthorizationMethodVisitor<T> methodVisitor)
    {
        return methodVisitor.Visit(this);
    }
}