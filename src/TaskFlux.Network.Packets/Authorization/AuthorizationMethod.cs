namespace TaskFlux.Network.Packets.Authorization;

public abstract class AuthorizationMethod
{
    public abstract AuthorizationMethodType AuthorizationMethodType { get; }

    internal AuthorizationMethod()
    {
    }

    public abstract void Accept(IAuthorizationMethodVisitor methodVisitor);

    public abstract ValueTask AcceptAsync(IAsyncAuthorizationMethodVisitor methodVisitor,
                                          CancellationToken token = default);
}