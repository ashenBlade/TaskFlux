namespace TaskFlux.Persistence.ApplicationState.Deltas;

/// <summary>
/// Исключение, полученное при десериализации неизвестного типа дельты
/// </summary>
public class UnknownDeltaTypeException : Exception
{
    /// <summary>
    /// Прочитанный тип дельты
    /// </summary>
    public byte Type { get; }

    public UnknownDeltaTypeException(byte type)
    {
        Type = type;
    }
}