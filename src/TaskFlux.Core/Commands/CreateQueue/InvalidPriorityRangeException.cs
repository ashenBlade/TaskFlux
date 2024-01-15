namespace TaskFlux.Core.Commands.CreateQueue;

/// <summary>
/// Исключение, возникающее, когда передан неправильный диапазон значений
/// </summary>
public class InvalidPriorityRangeException : Exception
{
    /// <summary>
    /// Переданный диапазон значений
    /// </summary>
    public (long Left, long Right) Range { get; }

    public InvalidPriorityRangeException(long min, long max)
    {
        Range = ( min, max );
    }
}