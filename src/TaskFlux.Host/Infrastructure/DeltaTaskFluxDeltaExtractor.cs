using Consensus.Raft;
using TaskFlux.Commands;

namespace TaskFlux.Host.Infrastructure;

public class TaskFluxDeltaExtractor : IDeltaExtractor<Response>
{
    public bool TryGetDelta(Response command, out byte[] delta)
    {
        if (command.TryGetDelta(out var d))
        {
            delta = d.Serialize();
            return true;
        }

        delta = Array.Empty<byte>();
        return false;
    }
}