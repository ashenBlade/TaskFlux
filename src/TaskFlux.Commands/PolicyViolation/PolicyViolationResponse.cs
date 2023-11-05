using System.Diagnostics;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;

namespace TaskFlux.Commands.PolicyViolation;

public class PolicyViolationResponse : Response
{
    public override ResponseType Type => ResponseType.PolicyViolation;
    public QueuePolicy ViolatedPolicy { get; }

    public PolicyViolationResponse(QueuePolicy violatedPolicy)
    {
        Debug.Assert(violatedPolicy is not null,
            "violatedPolicy is not null",
            "Объект нарушенной политики не может быть null");

        ViolatedPolicy = violatedPolicy;
    }


    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncResponseVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}