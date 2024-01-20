using Serilog;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Core;

namespace TaskFlux.Consensus.Tests.Infrastructure;

public class RaftConsensusModule : RaftConsensusModule<int, int>
{
    internal RaftConsensusModule(NodeId id,
                                 PeerGroup peerGroup,
                                 ILogger logger,
                                 ITimerFactory timerFactory,
                                 IBackgroundJobQueue backgroundJobQueue,
                                 FileSystemPersistenceFacade persistence,
                                 IDeltaExtractor<int> deltaExtractor,
                                 IApplicationFactory<int, int> applicationFactory)
        : base(id, peerGroup, logger, timerFactory, backgroundJobQueue, persistence,
            deltaExtractor, applicationFactory)
    {
    }
}