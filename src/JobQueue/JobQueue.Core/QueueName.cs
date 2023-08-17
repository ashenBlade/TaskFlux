using System.Runtime.CompilerServices;
using JobQueue.Core.Exceptions;

[assembly: InternalsVisibleTo("JobQueue.Core.Tests")]
[assembly: InternalsVisibleTo("TaskFlux.Commands.Serialization.Tests")]

namespace JobQueue.Core;

/// <summary>
/// Объект представляющий название очереди, удовлетворяющее бизнес-логике
/// </summary>
public struct QueueName
{
    public static QueueName Default => new(DefaultName);
    public const string DefaultName = "";
    
    public bool IsDefaultQueue => Name == DefaultName;
    public string Name { get; }
    
    private QueueName(string name)
    {
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

    public override string ToString()
    {
        return $"QueueName({Name})";
    }

    public override int GetHashCode()
    {
        return Name.GetHashCode();
    }

    public static bool TryParse(string raw, out QueueName name)
    {
        if (IsValidQueueName(raw))
        {
            name = new QueueName(raw);
            return true;
        }

        name = default!;
        return false;
    }

    private const int MaxNameLength = 255;
    
    private static bool IsValidQueueName(ReadOnlySpan<char> span)
    {
        // Можно через regex, но так быстрее
        
        if (MaxNameLength < span.Length)
        {
            return false;
        }

        // Использовать Encoding.ASCII.GetBytes(), а потом проверять каждый байт нельзя, 
        // т.к. при обнаружении байта больше 127, он декодируется в "?", 
        // который кодируется числом 65 и считается валидным.
        // Вместо этого каждый символ лучше проверять отдельно
        
        // .NET использует UTF-16 для кодировки строк,
        // кодировка первых 127 символов одинаковы для UTF-8 и ASCII.
        // Поэтому можно использовать строку .NET для проверки значений до 127 
        // и при необходимости сериализовывать в нужные кодировки (ASCII и т.д.)
        
        foreach (var ch in span)
        {
            // Выбранные значения образуют непрерывный возрастающий диапазон значений [33; 126]
            if (ch is < '!' /* 33  */  
                   or > '~' /* 126 */)
            {
                return false;
            }
        }
        
        return true;
    }

    // В кодировке UTF-8 (+ ASCII и UTF-16) - возврастающий диапазон [33; 126]
    internal const string AllowedCharacters =
        "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";

    /// <summary>
    /// Спарсить название строки и вернуть объект названия очереди
    /// </summary>
    /// <param name="raw">Сырая строка</param>
    /// <returns>Объект названия очереди</returns>
    /// <exception cref="InvalidQueueNameException">Переданная строка была в неверном формате</exception>
    /// <exception cref="ArgumentNullException"><paramref name="raw"/> - <c>null</c></exception>
    public static QueueName Parse(string raw)
    {
        ArgumentNullException.ThrowIfNull(raw);
        
        if (TryParse(raw, out var queueName))
        {
            return queueName;
        }

        // MAYBE: переделать логику TryParse, чтобы он сам выкидывал исключения + там же и сделать их дескриптивными.
        // например - передавать флаг bool safe и если выставлен, то не кидать исключения, иначе исключение с пояснением
        throw new InvalidQueueNameException(raw);
    }

    /// <summary>
    /// Создать случайное название для очереди
    /// </summary>
    /// <remarks>
    /// Для тестов (может потом фичу такую запилим, но сейчас только для тестов)
    /// </remarks>
    /// <param name="length">Длина очереди. Допустимые значения: <br/> -1 - случайная длина, 0-255 - допустимые значения</param>
    /// <returns>Очередь со случайным названием</returns>
    internal static QueueName GenerateRandomQueueName(int length = 0)
    {
        if (length is < -1 or > 255)
        {
            throw new ArgumentOutOfRangeException(nameof( length ), $"Либо -1 для случайной длины, либо среди [0; 255]. Передано: {length}");
        }
        
        length = length == -1
                     ? Random.Shared.Next(0, byte.MaxValue + 1)
                     : length;
        
        var name = string.Create(length, new Random(), (span, rnd) =>
        {
            for (int i = 0; i < span.Length; i++)
            {
                span[i] = AllowedCharacters[rnd.Next(0, AllowedCharacters.Length)];
            }
        });
        
        return new QueueName(name);
    }
}