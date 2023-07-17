using Raft.Core;

namespace Raft.Host.Infrastructure;

public static class NodeExtensions
{
    public static bool IsLeader(this IConsensusModule consensusModule) => consensusModule.CurrentRole == NodeRole.Leader;
}