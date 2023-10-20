using System.Net;

namespace TaskFlux.Core;

public interface IClusterInfo
{
    /// <summary>
    /// Id кластера
    /// </summary>
    public NodeId? LeaderId { get; }

    /// <summary>
    /// Список из узлов кластера и их адресов.
    /// Индекс соответствует Id узла
    /// </summary>
    public IReadOnlyList<EndPoint> Nodes { get; }

    /// <summary>
    /// Id текущего узла
    /// </summary>
    public NodeId CurrentNodeId { get; }
}