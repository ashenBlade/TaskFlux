using System.Diagnostics;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("TaskQueue.Core.Tests")]
[assembly: InternalsVisibleTo("TaskFlux.Commands.Serialization.Tests")]

namespace TaskFlux.Core;

/// <summary>
/// Объект представляющий название очереди, удовлетворяющее бизнес-логике
/// </summary>
public readonly struct QueueName : IEquatable<QueueName>
{
    public static QueueName Default => new(DefaultName);
    public const string DefaultName = "";

    public bool IsDefaultQueue => Name == DefaultName;
    public string Name { get; }

    internal QueueName(string name)
    {
        Debug.Assert(name != null, "Название очереди не может быть null");
        Name = name;
    }

    public QueueName()
    {
        Name = DefaultName;
    }

    public static implicit operator string(QueueName queueName)
    {
        return queueName.Name;
    }

    public static QueueName Parse(string name)
    {
        return QueueNameParser.Parse(name);
    }

    public override string ToString()
    {
        return $"QueueName({Name})";
    }

    public override int GetHashCode()
    {
        return Name.GetHashCode();
    }

    public bool Equals(QueueName other)
    {
        return Name == other.Name;
    }

    public override bool Equals(object? obj)
    {
        return obj is QueueName other && Equals(other);
    }

    public static bool operator ==(QueueName left, QueueName right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(QueueName left, QueueName right)
    {
        return !( left == right );
    }

    /// <summary>
    /// Создать случайное название очереди
    /// </summary>
    /// <param name="length">Длина названия</param>
    /// <returns>Созданное название очереди</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> меньше 0, либо больше максимального значения (<see cref="QueueNameParser.MaxNameLength"/>)</exception>
    public static QueueName CreateRandom(int length)
    {
        if (length < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(length), length,
                "Длина названия очереди не может быть отрицательной");
        }

        if (QueueNameParser.MaxNameLength < length)
        {
            throw new ArgumentOutOfRangeException(nameof(length), length,
                "Длина названия очереди не может превышать максимальное значение");
        }

        return new QueueName(string.Create(length, Random.Shared, static (span, rnd) =>
        {
            rnd.GetItems(QueueNameParser.AllowedCharacters, span);
        }));
    }
}