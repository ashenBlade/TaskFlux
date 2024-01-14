using System.Net;
using Consensus.Raft;
using TaskFlux.Commands;
using TaskFlux.Models;
using TaskFlux.Transport.Common;

namespace TaskFlux.Host.Infrastructure;

public class ProxyApplicationInfo : IApplicationInfo
{
    private readonly IRaftConsensusModule<Command, Response> _module;
    private readonly EndPoint[] _clusterNodesAddresses;

    public ProxyApplicationInfo(IRaftConsensusModule<Command, Response> module, EndPoint[] clusterNodesAddresses)
    {
        _module = module;
        _clusterNodesAddresses = clusterNodesAddresses;
    }

    public Version ApplicationVersion => new Version(1, 0);
    public IReadOnlyList<EndPoint> Nodes => _clusterNodesAddresses;
    public NodeId? LeaderId => _module.LeaderId;
    public NodeId NodeId => _module.Id;
}