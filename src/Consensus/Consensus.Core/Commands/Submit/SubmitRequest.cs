using Consensus.StateMachine;

namespace Consensus.Core.Commands.Submit;

public record SubmitRequest<TRequest>(CommandDescriptor<TRequest> Descriptor);