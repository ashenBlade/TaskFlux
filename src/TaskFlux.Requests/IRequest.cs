using TaskFlux.Core;

namespace TaskFlux.Requests;

public interface IRequest
{
    public void Accept(IRequestVisitor visitor);
}