namespace TaskFlux.Network.Packets.Authorization;

public interface IAsyncAuthorizationMethodVisitor
{
    public ValueTask VisitAsync(NoneAuthorizationMethod noneAuthorizationMethod, CancellationToken token);
}