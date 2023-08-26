using TaskFlux.Core;

namespace Consensus.Raft;

public delegate void RoleChangedEventHandler(NodeRole oldRole, NodeRole newRole);