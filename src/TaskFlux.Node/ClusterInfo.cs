using System.Net;
using Consensus.Raft;
using TaskFlux.Commands;
using TaskFlux.Core;
using TaskFlux.Models;

namespace TaskFlux.Node;

public class ClusterInfo : IClusterInfo
{
    private readonly IRaftConsensusModule<Command, Response> _module;
    public NodeId? LeaderId => _module.LeaderId;
    public NodeId CurrentNodeId => _module.Id;
    public IReadOnlyList<EndPoint> Nodes { get; }

    public ClusterInfo(IEnumerable<EndPoint> nodes, IRaftConsensusModule<Command, Response> module)
    {
        _module = module;
        Nodes = nodes.ToArray();
    }

    public override string ToString()
    {
        return $"ClusterInfo(LeaderId = {LeaderId})";
    }
}