using Raft.StateMachine;

namespace Raft.Core.Commands.Submit;

/// <summary>
/// Передать узлу команду на применение команды
/// </summary>
/// <param name="WasLeader">Был ли узел лидером, маркер корректности ответа</param>
/// <param name="Response">Ответ от узла</param>
/// <remarks>Если узел не был лидером, то <see cref="Response"/> равен <see cref="NullResponse"/></remarks>
public record SubmitResponse(bool WasLeader, IResponse Response)
{
    public static readonly SubmitResponse NotALeader = new(false, NullResponse.Instance);

    public static SubmitResponse Success(IResponse response) => new(true, response);
}