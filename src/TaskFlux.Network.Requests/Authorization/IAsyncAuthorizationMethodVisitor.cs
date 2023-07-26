namespace TaskFlux.Network.Requests.Authorization;

public interface IAsyncAuthorizationMethodVisitor
{
    public ValueTask VisitAsync(NoneAuthorizationMethod noneAuthorizationMethod, CancellationToken token);
}