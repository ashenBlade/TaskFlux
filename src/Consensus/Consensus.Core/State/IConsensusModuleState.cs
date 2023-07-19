using Consensus.Core.Commands.AppendEntries;
using Consensus.Core.Commands.RequestVote;
using Consensus.Core.Commands.Submit;

namespace Consensus.Core.State;

/// <summary>
/// Интерфейс, представляющий конкретное состояние узла
/// </summary>
/// <remarks>
/// IDisposable нужно вызывать для сброса таймеров и очистки ресурсов предыдущего состояния (отписка от таймеров и т.д.)
/// </remarks>
internal interface IConsensusModuleState<TRequest, TResponse>: IDisposable
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
    public SubmitResponse<TResponse> Apply(SubmitRequest<TRequest> request);
}