namespace TaskFlux.Consensus;

/// <summary>
/// Передать узлу команду на применение команды
/// </summary>
/// <remarks>Если узел не был лидером, то <see cref="Response"/> равен <see cref="NullResponse"/></remarks>
public class SubmitResponse<TResponse>
{
    /// <summary>
    /// Передать узлу команду на применение команды
    /// </summary>
    /// <param name="response">Ответ от узла</param>
    /// <param name="wasLeader">Был ли узел лидером при ответе</param>
    /// <param name="hasValue">Выставлено ли значение для ответа</param>
    /// <remarks>Если узел не был лидером, то <see cref="Response"/> равен <see cref="NullResponse"/></remarks>
    private SubmitResponse(TResponse? response, bool wasLeader, bool hasValue)
    {
        Response = response;
        WasLeader = wasLeader;
        HasValue = hasValue;
    }

    /// <summary>Ответ от узла</summary>
    public TResponse? Response { get; init; }

    public bool TryGetResponse(out TResponse response)
    {
        if (HasValue)
        {
            response = Response!;
            return true;
        }

        response = default!;
        return false;
    }

    public bool WasLeader { get; init; }
    public bool HasValue { get; init; }

    public static SubmitResponse<TResponse> Success(TResponse response, bool wasLeader) =>
        new(response, wasLeader, true);

    public static readonly SubmitResponse<TResponse> NotLeader = new(default, false, false);
}