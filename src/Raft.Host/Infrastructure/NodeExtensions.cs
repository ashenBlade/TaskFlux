using Raft.Core;
using Raft.Core.Node;

namespace Raft.Host.Infrastructure;

public static class NodeExtensions
{
    public static bool IsLeader(this INode node) => node.CurrentRole == NodeRole.Leader;
}