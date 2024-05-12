using System.Text;
using TaskFlux.Core;
using TaskFlux.Core.Exceptions;

namespace TaskFlux.Domain;

/// <summary>
/// Вспомогательный класс для парсинга названия очереди
/// </summary>
public static class QueueNameParser
{
    public const int MaxNameLength = 255;

    /// <summary>
    /// Попытаться спарсить название очереди
    /// </summary>
    /// <param name="raw">Сырая строка названия</param>
    /// <param name="name">Полученное название очереди</param>
    /// <returns><c>true</c> - название было спарсено успешно, <c>false</c> - были ошибки при парсинге</returns>
    /// <remarks>Если функция вернула <c>false</c>, то <paramref name="name"/> может содержать мусор</remarks>
    public static bool TryParse(string raw, out QueueName name)
    {
        ArgumentNullException.ThrowIfNull(raw);

        Span<byte> buffer = stackalloc byte[MaxNameLength];
        int encoded;
        try
        {
            encoded = Encoding.GetBytes(raw, buffer);
        }
        catch (EncoderFallbackException)
        {
            name = QueueName.Default;
            return false;
        }

        if (IsValidQueueName(buffer[..encoded]))
        {
            name = new QueueName(raw);
            return true;
        }

        name = QueueName.Default;
        return false;
    }

    private const char MinChar = '!';
    private const char MaxChar = '~';

    private const byte MinByte = (byte)MinChar;
    private const byte MaxByte = (byte)MaxChar;

    // В кодировке UTF-8 (+ ASCII и UTF-16) - возврастающий диапазон [33; 126]
    internal const string AllowedCharacters =
        "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";

    /// <summary>
    /// Спарсить название строки и вернуть объект названия очереди
    /// </summary>
    /// <param name="raw">Сырая строка</param>
    /// <returns>Объект названия очереди</returns>
    /// <exception cref="ArgumentNullException">Переданная строка была в неверном формате</exception>
    /// <exception cref="raw"><paramref name="raw"/> - <c>null</c></exception>
    public static QueueName Parse(string raw)
    {
        ArgumentNullException.ThrowIfNull(raw);

        if (TryParse(raw, out var name))
        {
            return name;
        }

        // MAYBE: переделать логику TryParse, чтобы он сам выкидывал исключения + там же и сделать их дескриптивными.
        // например - передавать флаг bool safe и если выставлен, то не кидать исключения, иначе исключение с пояснением
        throw new InvalidQueueNameException(raw);
    }

    /// <summary>
    /// Кодировка, используемая для работы с названием очереди
    /// </summary>
    public static readonly Encoding Encoding = Encoding.GetEncoding("ascii",
        EncoderFallback.ExceptionFallback,
        DecoderFallback.ExceptionFallback);

    public static QueueName Parse(ReadOnlySpan<byte> span)
    {
        if (MaxNameLength < span.Length)
        {
            throw new InvalidQueueNameException();
        }

        if (span.Length == 0)
        {
            return QueueName.Default;
        }

        if (TryParse(span, out var name))
        {
            return name;
        }

        throw new InvalidQueueNameException();
    }


    private static bool TryParse(in ReadOnlySpan<byte> span, out QueueName name)
    {
        if (IsValidQueueName(span))
        {
            name = new QueueName(Encoding.GetString(span));
            return true;
        }

        name = QueueName.Default;
        return false;
    }

    private static bool IsValidQueueName(in ReadOnlySpan<byte> span)
    {
        // Проверим на максимальную длину - 255
        // Можно не проверять на 0
        if (MaxNameLength < span.Length)
        {
            return false;
        }

        for (var i = 0; i < span.Length; i++)
        {
            // Выбранный диапазон допустимых значений образует непрерывный отрезок
            // Начиная от 33 (!), заканчивая 126 (~).

            // В первых 127 символах страниц кодировок UTF-8, UTF-16 и ASCII одинаковые символы с кодами,
            // Поэтому можно проверять и не беспокоиться, т.к. кроме них в приложении другие не используются
            var b = span[i];
            // [33; 126]
            if (b is < MinByte
                or > MaxByte)
            {
                return false;
            }
        }

        return true;
    }
}