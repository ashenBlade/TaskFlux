using TaskFlux.Application.Persistence;
using TaskFlux.Consensus;
using TaskFlux.Core.Commands;

namespace TaskFlux.Host.Infrastructure;

public class TaskFluxDeltaExtractor : IDeltaExtractor<Response>
{
    private readonly DeltaExtractorResponseVisitor _visitor = new();

    public bool TryGetDelta(Response response, out byte[] deltaBytes)
    {
        var delta = response.Accept(_visitor);
        if (delta is null)
        {
            deltaBytes = Array.Empty<byte>();
            return false;
        }

        deltaBytes = delta.Serialize();
        return true;
    }
}