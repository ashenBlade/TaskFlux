using TaskFlux.Consensus;
using TaskFlux.Core.Commands;
using TaskFlux.Persistence.ApplicationState;

namespace TaskFlux.Host.Infrastructure;

public class TaskFluxDeltaExtractor : IDeltaExtractor<Command>
{
    private readonly DeltaExtractorCommandVisitor _visitor = new();

    public bool TryGetDelta(Command command, out byte[] deltaBytes)
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