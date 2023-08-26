using Consensus.Raft;
using TaskFlux.Core;

namespace TaskFlux.Host.Infrastructure;

public static class NodeExtensions
{
    public static bool IsLeader<TCommand, TResponse>(this IConsensusModule<TCommand, TResponse> consensusModule) => consensusModule.CurrentRole == NodeRole.Leader;
}