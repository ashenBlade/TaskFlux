namespace TaskFlux.Requests;

public interface IResponse
{
    public void Accept(IResponseVisitor visitor);
}