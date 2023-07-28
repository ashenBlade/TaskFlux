namespace TaskFlux.Network.Requests.Authorization;

public interface IAuthorizationMethodVisitor
{
    public void Visit(NoneAuthorizationMethod noneAuthorizationMethod);
}