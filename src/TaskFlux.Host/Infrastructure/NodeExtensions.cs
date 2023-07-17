using Consensus.Core;

namespace TaskFlux.Host.Infrastructure;

public static class NodeExtensions
{
    public static bool IsLeader(this IConsensusModule consensusModule) => consensusModule.CurrentRole == NodeRole.Leader;
}