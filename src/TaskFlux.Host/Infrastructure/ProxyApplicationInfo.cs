using System.Net;
using TaskFlux.Application;
using TaskFlux.Consensus;
using TaskFlux.Core;
using TaskFlux.Core.Commands;

namespace TaskFlux.Host.Infrastructure;

public class ProxyApplicationInfo : IApplicationInfo
{
    private readonly RaftConsensusModule<Command, Response> _module;
    private readonly EndPoint[] _clusterNodesAddresses;

    public ProxyApplicationInfo(RaftConsensusModule<Command, Response> module, EndPoint[] clusterNodesAddresses)
    {
        _module = module;
        _clusterNodesAddresses = clusterNodesAddresses;
    }

    public Version ApplicationVersion => new(1, 0);
    public IReadOnlyList<EndPoint> Nodes => _clusterNodesAddresses;
    public NodeId? LeaderId => _module.LeaderId;
    public NodeId NodeId => _module.Id;
}