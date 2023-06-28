using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Commands.Submit;

namespace Raft.Core.Node;

/// <summary>
/// Интерфейс, представляющий конкретное состояние узла
/// </summary>
/// <remarks>
/// IDisposable нужно вызывать для сброса таймеров и очистки ресурсов предыдущего состояния (отписка от таймеров и т.д.)
/// </remarks>
internal interface INodeState: IDisposable
{
    /// <summary>
    /// Текущая роль этого состояния
    /// </summary>
    public NodeRole Role { get; }
    /// <summary>
    /// Применить команду RequestVote
    /// </summary>
    /// <param name="request">Объект запроса</param>
    /// <returns>Ответ узла</returns>
    public RequestVoteResponse Apply(RequestVoteRequest request);

    /// <summary>
    /// Применить команду AppendEntries
    /// </summary>
    /// <param name="request">Объект запроса</param>
    /// <returns>Ответ узла</returns>
    public AppendEntriesResponse Apply(AppendEntriesRequest request);

    /// <summary>
    /// Применить команду к машине состояний
    /// </summary>
    /// <param name="request">Объект запроса</param>
    /// <returns>Результат операции</returns>
    public SubmitResponse Apply(SubmitRequest request);
}