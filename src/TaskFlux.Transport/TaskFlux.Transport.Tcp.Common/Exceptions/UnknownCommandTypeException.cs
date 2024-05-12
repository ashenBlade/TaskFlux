using TaskFlux.Network.Commands;

namespace TaskFlux.Network.Exceptions;

/// <summary>
/// Исключение, возникающее когда прочитан неизвестный маркер <see cref="NetworkCommand"/>
/// </summary>
public class UnknownCommandTypeException : Exception
{
    /// <summary>
    /// Маркер команды
    /// </summary>
    public byte CommandType { get; }

    public UnknownCommandTypeException(byte commandType)
    {
        CommandType = commandType;
    }
}