using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.Error;

public class ErrorResponse : Response
{
    public string Message { get; }
    public ErrorType ErrorType { get; }

    public ErrorResponse(ErrorType type, string message)
    {
        Message = message;
        ErrorType = type;
    }

    public override ResponseType Type => ResponseType.Error;

    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}