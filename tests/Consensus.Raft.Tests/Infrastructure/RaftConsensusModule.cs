using Consensus.CommandQueue;
using Consensus.Raft.Persistence;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Raft.Tests.Infrastructure;

public class RaftConsensusModule : RaftConsensusModule<int, int>
{
    internal RaftConsensusModule(NodeId id,
                                 PeerGroup peerGroup,
                                 ILogger logger,
                                 ITimer electionTimer,
                                 ITimer heartbeatTimer,
                                 IBackgroundJobQueue backgroundJobQueue,
                                 StoragePersistenceFacade persistenceFacade,
                                 ICommandQueue commandQueue,
                                 IStateMachine<int, int> stateMachine,
                                 ICommandSerializer<int> commandSerializer,
                                 IStateMachineFactory<int, int> stateMachineFactory) : base(id, peerGroup, logger,
        electionTimer, heartbeatTimer, backgroundJobQueue, persistenceFacade, commandQueue, stateMachine,
        commandSerializer, stateMachineFactory)
    {
    }
}