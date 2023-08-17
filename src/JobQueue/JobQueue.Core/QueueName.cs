using System.Runtime.CompilerServices;
using System.Text;

[assembly: InternalsVisibleTo("JobQueue.Core.Tests")]

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
    /// <exception cref="FormatException">Переданная строка была в неверном формате</exception>
    /// <exception cref="ArgumentNullException"><paramref name="raw"/> - <c>null</c></exception>
    public static QueueName Parse(string raw)
    {
        ArgumentNullException.ThrowIfNull(raw);
        
        if (TryParse(raw, out var queueName))
        {
            return queueName;
        }

        throw new FormatException("Ошибка парсинга названия очереди");
    }
}