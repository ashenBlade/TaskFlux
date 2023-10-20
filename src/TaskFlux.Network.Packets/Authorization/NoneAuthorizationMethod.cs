namespace TaskFlux.Network.Packets.Authorization;

public class NoneAuthorizationMethod : AuthorizationMethod
{
    public override AuthorizationMethodType AuthorizationMethodType => AuthorizationMethodType.None;
    public static readonly NoneAuthorizationMethod Instance = new();

    public override void Accept(IAuthorizationMethodVisitor methodVisitor)
    {
        methodVisitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncAuthorizationMethodVisitor methodVisitor,
                                          CancellationToken token = default)
    {
        return methodVisitor.VisitAsync(this, token);
    }
}