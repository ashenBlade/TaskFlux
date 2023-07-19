using Consensus.StateMachine;

namespace Consensus.Core.Commands.Submit;

/// <summary>
/// Передать узлу команду на применение команды
/// </summary>
/// <param name="WasLeader">Был ли узел лидером, маркер корректности ответа</param>
/// <param name="Response">Ответ от узла</param>
/// <remarks>Если узел не был лидером, то <see cref="Response"/> равен <see cref="NullResponse"/></remarks>
public record SubmitResponse<TResponse>(bool WasLeader, TResponse Response)
{
    public static readonly SubmitResponse<TResponse> NotALeader = new(false, default!);

    public static SubmitResponse<TResponse> Success(TResponse response) => new(true, response);
}