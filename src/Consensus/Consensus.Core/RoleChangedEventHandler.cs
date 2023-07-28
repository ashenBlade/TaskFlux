using TaskFlux.Core;

namespace Consensus.Core;

public delegate void RoleChangedEventHandler(NodeRole oldRole, NodeRole newRole);