using Consensus.Raft;
using TaskFlux.Commands;

namespace TaskFlux.Host.Infrastructure;

public class DeltaTaskFluxCommandSerializer : ICommandSerializer<Command>
{
    public bool TryGetDelta(Command command, out byte[] delta)
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