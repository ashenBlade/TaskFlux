namespace Consensus.Raft.Commands.Submit;

public record SubmitRequest<TRequest>(CommandDescriptor<TRequest> Descriptor);