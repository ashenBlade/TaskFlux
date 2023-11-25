using System.Buffers;
using System.Diagnostics;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Core.Queue;
using TaskFlux.PriorityQueue;
using Utils.Serialization;

namespace TaskFlux.Commands.Serialization;

/// <summary>
/// Вспомогательный класс для сериализации и десериализации <see cref="ListQueuesResponse"/>
/// </summary>
internal static class MetadataSerializerHelpers
{
    // Основные заголовки для метаданных
    private const string MaxSizeHeader = "max-queue-size";
    private const string MaxPayloadSizeHeader = "max-payload-size";
    private const string PriorityRangeHeader = "priority-range";

    // Закэшированная длина сериализованных заголовков 
    private static readonly int MaxSizeHeaderSize = MemoryBinaryWriter.EstimateResultSize(MaxSizeHeader);
    private static readonly int MaxPayloadSizeHeaderSize = MemoryBinaryWriter.EstimateResultSize(MaxPayloadSizeHeader);
    private static readonly int PriorityRangeHeaderSize = MemoryBinaryWriter.EstimateResultSize(PriorityRangeHeader);

    public static byte[] SerializeMetadata(IReadOnlyCollection<ITaskQueueMetadata> metadata)
    {
        // Создаем удобную обертку для подсчета
        var infos = ExtractInfo(metadata);

        // Вычисляем требуемый размер буфера и одновременно делаем нужные подсчеты
        var size = EstimateTotalSize(infos);

        // Аллоцируем временный массив для сериализованных данных
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            // Сама сериализация
            var memory = buffer.AsMemory(0, size);
            var writer = new MemoryBinaryWriter(memory);

            // Маркерный байт
            writer.Write(( byte ) ResponseType.ListQueues);

            // Количество сериализованных элементов
            writer.Write(infos.Length);

            for (var i = 0; i < infos.Length; i++)
            {
                infos[i].WriteTo(ref writer);
            }

            // Возвращаем, что получили
            return memory.ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public static IReadOnlyList<ITaskQueueMetadata> DeserializeMetadata(ref ArrayBinaryReader reader)
    {
        var count = reader.ReadInt32();
        Debug.Assert(count >= 0, "count >= 0", "Количество записей не должно быть отрицательным");
        if (count == 0)
        {
            return Array.Empty<ITaskQueueMetadata>();
        }

        var result = new ITaskQueueMetadata[count];

        for (int i = 0; i < count; i++)
        {
            var builder = new PlainTaskQueueMetadata.Builder();

            // 1. Название очереди
            builder.WithQueueName(reader.ReadQueueName());

            // 2. Реализация 
            builder.WithPriorityQueueCode(( PriorityQueueCode ) reader.ReadInt32());

            // 3. Количество элементов в ней
            builder.WithCount(reader.ReadInt32());

            // 4. Сколько там заголовков
            var headersCount = reader.ReadInt32();

            if (headersCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(headersCount), headersCount,
                    "Количество заголовков в очереди не может быть отрицательным");
            }

            if (headersCount == 0)
            {
                result[i] = builder.Build();
                continue;
            }

            // 5. Парсим каждый заголовок
            for (var j = 0; j < headersCount; j++)
            {
                switch (reader.ReadString())
                {
                    case MaxSizeHeader:
                        if (reader.ReadString() is {Length: > 0} maxSizeHeaderValue)
                        {
                            builder.WithMaxSize(ParseInt(maxSizeHeaderValue, MaxSizeHeader));
                        }

                        break;
                    case MaxPayloadSizeHeader:
                        if (reader.ReadString() is {Length: > 0} maxPayloadSizeHeaderValue)
                        {
                            builder.WithMaxPayloadSize(ParseInt(maxPayloadSizeHeaderValue, MaxPayloadSizeHeader));
                        }

                        break;
                    case PriorityRangeHeader:
                        if (reader.ReadString() is {Length: > 0} priorityRangeHeaderValue)
                        {
                            builder.WithPriorityRange(ParsePriorityRange(priorityRangeHeaderValue));
                        }

                        break;
                    default:
                        // Неизвестный заголовок, возможно другая версия
                        _ = reader.ReadString();
                        break;
                }
            }

            result[i] = builder.Build();
        }

        return result;
    }

    private static (long, long) ParsePriorityRange(string priorityRangeString)
    {
        // Строка диапазона представляется 2 числами, разделенными '-'
        var spaceIndex = priorityRangeString.IndexOf(' ');
        if (spaceIndex == -1)
        {
            throw new ArgumentException(
                "Неправильный формат диапазона приоритетов: отсутствует пробел между значениями");
        }

        try
        {
            var min = long.Parse(priorityRangeString.AsSpan(0, spaceIndex));
            var max = long.Parse(priorityRangeString.AsSpan(spaceIndex + 1));
            return ( min, max );
        }
        catch (FormatException fe)
        {
            throw new ArgumentException(
                $"Неправильный формат диапазона приоритетов: числа представлены в неправильном формате.\nУказанный диапазон: {priorityRangeString}",
                fe);
        }
        catch (OverflowException oe)
        {
            throw new ArgumentException(
                $"Неправильный формат диапазона приоритетов: указанные числа выходят за пределы возможных занчений.\nУказанный диапазон: {priorityRangeString}",
                oe);
        }
    }

    private static int ParseInt(string value, string header)
    {
        try
        {
            return int.Parse(value);
        }
        catch (FormatException fe)
        {
            throw new ArgumentException(
                $"Ошибка при парсинге строки \"{value}\" заголовка {header} - число представлено в неправильном формате",
                fe);
        }
        catch (OverflowException oe)
        {
            throw new ArgumentException(
                $"Ошибка при парсинге строки \"{value}\" заголовка {header} - число слишком большое", oe);
        }
    }

    private static int EstimateTotalSize(QueueMetadataInfo[] infos)
    {
        var serializedDataSize = 0;
        for (var i = 0; i < infos.Length; i++)
        {
            serializedDataSize += infos[i].EstimateMetadataSize();
        }

        return sizeof(int)          // Количество метаданных (длина массива)
             + sizeof(ResponseType) // Маркерный байт
             + serializedDataSize;  // Сериализованные данные очередей
    }

    private static QueueMetadataInfo[] ExtractInfo(IReadOnlyCollection<ITaskQueueMetadata> metadata)
    {
        // Сразу аллоцируем массив нужного количества
        var result = new QueueMetadataInfo[metadata.Count];
        var index = 0;
        foreach (var md in metadata)
        {
            result[index] = new QueueMetadataInfo(md);
            index++;
        }

        return result;
    }

    /// <summary>
    /// Класс-обертка для операций по вычислению
    /// </summary>
    private struct QueueMetadataInfo
    {
        // Закешированные значения различных метаданных очереди (лишний раз не нагружать цп)
        private string? _maxSize;
        private string? _maxPayloadSize;
        private string? _priorityRange;

        // Количество указанных (не null) заголовков
        private int _headersCount = 0;

        public QueueMetadataInfo(ITaskQueueMetadata metadata)
        {
            Metadata = metadata;
        }

        /// <summary>
        /// Метаданные, которые мы считаем
        /// </summary>
        private ITaskQueueMetadata? Metadata { get; }

        /// <summary>
        /// Вычисление размера необходимого для серилазиации этих метаданных
        /// </summary>
        /// <returns>Размер в байтах</returns>
        public int EstimateMetadataSize()
        {
            if (Metadata is null)
            {
                return 0;
            }

            var size = 0;

            // Обязательные параметры
            size += MemoryBinaryWriter.EstimateResultSize(Metadata.QueueName) // Название очереди
                  + sizeof(int)                                               // Код реализации
                  + sizeof(int)                                               // Размер очереди
                  + sizeof(int);                                              // Количество заголовков 

            // Опциональные параметры - максимальный размер, диапазон ключей и т.д.
            if (Metadata.MaxQueueSize is { } maxSize)
            {
                size += MaxSizeHeaderSize
                      + MemoryBinaryWriter.EstimateResultSize(_maxSize = maxSize.ToString());
                _headersCount++;
            }

            if (Metadata.PriorityRange is var (min, max))
            {
                size += PriorityRangeHeaderSize
                      + MemoryBinaryWriter.EstimateResultSize(_priorityRange = $"{min} {max}");
                _headersCount++;
            }

            if (Metadata.MaxPayloadSize is { } maxPayloadSize)
            {
                size += MaxPayloadSizeHeaderSize
                      + MemoryBinaryWriter.EstimateResultSize(_maxPayloadSize = maxPayloadSize.ToString());
                _headersCount++;
            }

            return size;
        }

        public void WriteTo(ref MemoryBinaryWriter writer)
        {
            if (Metadata is null)
            {
                return;
            }

            // 1. Название очереди
            writer.Write(Metadata.QueueName);

            // 2. Реализация очереди
            writer.Write(( int ) Metadata.Code);

            // 3. Размер очереди
            writer.Write(Metadata.Count);

            // 4. Количество параметров
            writer.Write(_headersCount);

            if (_headersCount == 0)
            {
                // Параметров нет
                return;
            }

            // 5. Записываем сами параметры очереди
            if (_maxSize is { } maxSize)
            {
                writer.Write(MaxSizeHeader);
                writer.Write(maxSize);
            }

            if (_priorityRange is { } priorityRange)
            {
                writer.Write(PriorityRangeHeader);
                writer.Write(priorityRange);
            }

            if (_maxPayloadSize is { } maxPayloadSize)
            {
                writer.Write(MaxPayloadSizeHeader);
                writer.Write(maxPayloadSize);
            }
        }
    }
}