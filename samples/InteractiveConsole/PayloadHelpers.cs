using System.Text;

namespace InteractiveConsole;

public static class PayloadHelpers
{
    private static readonly Encoding Encoding = new UTF8Encoding(false, true);

    /// <summary>
    /// Сериализовать строку в байты нагрузки
    /// </summary>
    /// <param name="data">Строка, которую нужно сериализовать</param>
    /// <returns>Серализованная нагрузка</returns>
    public static byte[] Serialize(string data)
    {
        var size = Encoding.GetByteCount(data);
        var buffer = new byte[size];
        Encoding.GetBytes(data, buffer);
        return buffer;
    }

    /// <summary>
    /// Десериализовать байты нагрузки в строку из которой она была создана
    /// </summary>
    /// <param name="data">Данные нагрузки</param>
    /// <returns>Десериализованная строка</returns>
    /// <exception cref="DecoderFallbackException">Ошибка при декодировании строки</exception>
    public static string Deserialize(byte[] data)
    {
        return Encoding.GetString(data);
    }
}