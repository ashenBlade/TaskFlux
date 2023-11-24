using Consensus.Core;

namespace Consensus.Raft.Commands.Submit;

// TODO: заменить CommandDescriptor на TRequest и отдельный bool ShouldReplicate
public record SubmitRequest<TRequest>(CommandDescriptor<TRequest> Descriptor);