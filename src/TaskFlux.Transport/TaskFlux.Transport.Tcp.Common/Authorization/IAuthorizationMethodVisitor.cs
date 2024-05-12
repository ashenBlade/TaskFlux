namespace TaskFlux.Network.Authorization;

public interface IAuthorizationMethodVisitor
{
    public void Visit(NoneAuthorizationMethod noneAuthorizationMethod);
}

public interface IAuthorizationMethodVisitor<out T>
{
    public T Visit(NoneAuthorizationMethod noneAuthorizationMethod);
}