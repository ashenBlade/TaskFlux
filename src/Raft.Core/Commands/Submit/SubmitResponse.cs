using Raft.Core.Log;

namespace Raft.Core.Commands.Submit;

public record SubmitResponse(LogEntry CreatedEntry);