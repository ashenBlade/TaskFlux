namespace Consensus.Core.Commands.Submit;

public record SubmitRequest<TRequest>(CommandDescriptor<TRequest> Descriptor);