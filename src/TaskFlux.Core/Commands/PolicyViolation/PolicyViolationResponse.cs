using System.Diagnostics;
using TaskFlux.Core.Commands.Visitors;
using TaskFlux.Core.Policies;

namespace TaskFlux.Core.Commands.PolicyViolation;

public class PolicyViolationResponse : Response
{
    public override ResponseType Type => ResponseType.PolicyViolation;
    public QueuePolicy ViolatedPolicy { get; }

    public PolicyViolationResponse(QueuePolicy violatedPolicy)
    {
        Debug.Assert(violatedPolicy is not null,
            "violatedPolicy is not null",
            "Объект нарушенной политики не может быть null");
        ArgumentNullException.ThrowIfNull(violatedPolicy);

        ViolatedPolicy = violatedPolicy;
    }


    public override void Accept(IResponseVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override T Accept<T>(IResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}