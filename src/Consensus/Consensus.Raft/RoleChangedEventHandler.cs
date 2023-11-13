using TaskFlux.Models;

namespace Consensus.Raft;

public delegate void RoleChangedEventHandler(NodeRole oldRole, NodeRole newRole);