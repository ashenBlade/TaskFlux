using TaskFlux.Consensus;
using TaskFlux.Core.Commands;
using TaskFlux.Persistence.ApplicationState;

namespace TaskFlux.Host.Infrastructure;

public class TaskFluxDeltaExtractor : IDeltaExtractor<Response>
{
    private readonly DeltaExtractorResponseVisitor _visitor = new();

    public bool TryGetDelta(Response command, out byte[] deltaBytes)
    {
        var delta = command.Accept(_visitor);
        if (delta is null)
        {
            deltaBytes = Array.Empty<byte>();
            return false;
        }

        deltaBytes = delta.Serialize();
        return true;
    }
}