using System.Net;
using TaskFlux.Models;

namespace TaskFlux.Transport.Common;

public interface IApplicationInfo
{
    /// <summary>
    /// Текущая версия приложения
    /// </summary>
    public Version ApplicationVersion { get; }

    /// <summary>
    /// Адреса узлов кластера
    /// </summary>
    public IReadOnlyList<EndPoint> Nodes { get; }

    /// <summary>
    /// Id лидера кластера
    /// </summary>
    public NodeId? LeaderId { get; }

    /// <summary>
    /// Id текущего узла
    /// </summary>
    public NodeId NodeId { get; }
}