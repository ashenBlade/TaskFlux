namespace TaskFlux.Network.Packets.Authorization;

public interface IAuthorizationMethodVisitor
{
    public void Visit(NoneAuthorizationMethod noneAuthorizationMethod);
}