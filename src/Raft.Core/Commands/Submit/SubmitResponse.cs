using Raft.Core.Log;
using Raft.StateMachine;

namespace Raft.Core.Commands.Submit;

public record SubmitResponse(LogEntry CreatedEntry, IResponse Response);