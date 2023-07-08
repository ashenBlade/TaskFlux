using Raft.StateMachine;

namespace Raft.Core.Commands.Submit;

public record SubmitResponse(bool WasLeader, IResponse Response)
{
    public static readonly SubmitResponse NotALeader = new(false, NullResponse.Instance);

    public static SubmitResponse Success(IResponse response) => new(true, response);
}