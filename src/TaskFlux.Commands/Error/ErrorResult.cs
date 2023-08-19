using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.Error;

public class ErrorResult: Result
{
    public string Message { get; }
    public ErrorType ErrorType { get; }

    public ErrorResult(ErrorType type, string message)
    {
        Message = message;
        ErrorType = type;
    }
    
    public override ResultType Type => ResultType.Error;
    public override void Accept(IResultVisitor visitor)
    {
        visitor.Visit(this);    
    }

    public override ValueTask AcceptAsync(IAsyncResultVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}