using System.Diagnostics;
using System.IO.Abstractions;
using TaskFlux.Core;
using TaskFlux.Utils.CheckSum;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Persistence.Metadata;

public class MetadataFile
{
    public static Term DefaultTerm => Term.Start;
    public static NodeId? DefaultVotedFor => null;

    private const int Marker = 0x5A6EA012;

    private const int CurrentVersion = 1;

    /// <summary>
    /// Позиция начала данных: sizeof(Marker) + sizeof(CurrentVersion)
    /// </summary>
    private const long DataStartPosition = sizeof(int) + sizeof(int);

    /// <summary>
    /// Флаг указывающий на то, что голоса отдано не было.
    /// Записывается на месте Id узла
    /// </summary>
    private const int NoVotedFor = -1;

    /// <summary>
    /// Терм, чтобы использовать, если файл не был изначально инициализирован
    /// </summary>
    public Term Term { get; private set; }

    /// <summary>
    /// Голос, который будет использоваться, если файл не был изначально иницилазирован
    /// </summary>
    public NodeId? VotedFor { get; private set; }

    private readonly FileSystemStream _file;

    private MetadataFile(FileSystemStream file, Term term, NodeId? votedFor)
    {
        _file = file;
        VotedFor = votedFor;
        Term = term;
    }

    public void Update(Term term, NodeId? votedFor)
    {
        Debug.Assert(_file.Length > 0, "Файл должен быть инициализирован на момент вызова Update");

        Span<byte> span = stackalloc byte[FileSize];
        FillFile(term, votedFor, span);

        _file.Seek(0, SeekOrigin.Begin);
        _file.Write(span);
        _file.Flush(true);

        Term = term;
        VotedFor = votedFor;
    }

    /// <summary>
    /// Полный размер файла.
    /// Файл может быть либо пустым (размер 0), либо этого размера.
    /// Другие значения указывают на беды с башкой
    /// </summary>
    private const int FileSize = sizeof(int)   // Маркер 
                               + sizeof(int)   // Версия
                               + sizeof(long)  // Терм
                               + sizeof(int)   // Голос
                               + sizeof(uint); // CRC

    internal (Term, NodeId?) ReadStoredDataTest()
    {
        Span<byte> span = stackalloc byte[FileSize];
        _file.Seek(0, SeekOrigin.Begin);
        _file.ReadExactly(span);

        var reader = new SpanBinaryReader(span);
        var marker = reader.ReadInt32();
        if (marker != Marker)
        {
            throw new InvalidDataException(
                $"Прочитанный маркер не равен требуемому. Прочитанный: {marker}. Требуемый: {Marker}");
        }

        var version = reader.ReadInt32();
        if (version != CurrentVersion)
        {
            throw new InvalidDataException(
                $"Прочитанная версия не равна текущей. Прочитанная: {version}. Требуемая: {CurrentVersion}");
        }

        var term = new Term(reader.ReadInt64());
        var votedFor = ( NodeId? ) ( reader.ReadInt32() switch
                                     {
                                         NoVotedFor => null,
                                         var value  => new NodeId(value)
                                     } );
        var storedCheckSum = reader.ReadUInt32();
        var computedCheckSum = Crc32CheckSum.Compute(span[sizeof(int)..^sizeof(uint)]);
        if (storedCheckSum != computedCheckSum)
        {
            throw new InvalidDataException(
                $"Прочитанная чек-сумма не равна рассчитанная. Рассчитанная: {computedCheckSum}. Прочитанная: {storedCheckSum}");
        }

        return ( term, votedFor );
    }

    private static (Term Term, NodeId? VotedFor) InitializeFile(FileSystemStream file)
    {
        if (file.Length == 0)
        {
            Span<byte> span = stackalloc byte[FileSize];
            FillFile(DefaultTerm, null, span);
            file.Seek(0, SeekOrigin.Begin);
            file.Write(span);
            file.Flush(true);

            return ( DefaultTerm, DefaultVotedFor );
        }

        if (file.Length < FileSize)
        {
            // Пока указываю минимальную длину вместо фиксированной.
            // В будущем, возможно, появятся другие версии.
            // Надо сохранить совместимость.
            throw new InvalidDataException(
                $"Файл может быть либо пустым, либо не меньше фиксированной длины. Размер файла: {file.Length}. Минимальный размер: {FileSize}");
        }

        Span<byte> buffer = stackalloc byte[FileSize];
        file.Seek(0, SeekOrigin.Begin);
        file.ReadExactly(buffer);

        // Проверяем валидность данных на файле
        var reader = new SpanBinaryReader(buffer);
        var marker = reader.ReadInt32();
        if (marker != Marker)
        {
            throw new InvalidDataException(
                $"Хранившийся в файле маркер не равен требуемому. Считанный: {marker}. Требуемый: {Marker}");
        }

        var version = reader.ReadInt32();
        if (CurrentVersion < version)
        {
            throw new InvalidDataException(
                $"Хранившаяся в файле версия больше текущей. Считанная версия: {version}. Текущая версия: {CurrentVersion}");
        }

        var term = ReadTerm(ref reader);
        var votedFor = ReadVotedFor(ref reader);
        var storedCheckSum = reader.ReadUInt32();
        var computedCheckSum = Crc32CheckSum.Compute(buffer[sizeof(int)..^sizeof(uint)]);
        if (storedCheckSum != computedCheckSum)
        {
            throw new InvalidDataException(
                $"Прочитанная чек-сумма не равна вычисленной. Вычисленная: {computedCheckSum}. Прочитанная: {storedCheckSum}");
        }

        return ( term, votedFor );

        static Term ReadTerm(ref SpanBinaryReader r)
        {
            long? value = null;
            try
            {
                var termValue = r.ReadInt64();
                value = termValue;
                return new Term(termValue);
            }
            catch (ArgumentOutOfRangeException e)
            {
                throw new InvalidDataException(value is { } t
                                                   ? $"В файле был записан терм представляющий неверное значение: {t}"
                                                   : "В файле был записан терм представляющий неверное значение", e);
            }
        }

        static NodeId? ReadVotedFor(ref SpanBinaryReader r)
        {
            int? value = null;
            try
            {
                var read = r.ReadInt32();
                value = read;
                return read switch
                       {
                           NoVotedFor => null,
                           _          => new NodeId(read)
                       };
            }
            catch (ArgumentOutOfRangeException e)
            {
                throw new InvalidDataException(value is { } v
                                                   ? $"Значение отданного голоса представляет неверное значение: {v}"
                                                   : "Значение отданного голоса представляет неверное значение", e);
            }
        }
    }

    public static MetadataFile Initialize(IDirectoryInfo dataDirectory)
    {
        var metadataFile =
            dataDirectory.FileSystem.FileInfo.New(Path.Combine(dataDirectory.FullName, Constants.MetadataFileName));
        FileSystemStream fileStream;
        try
        {
            fileStream = metadataFile.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (Exception e)
        {
            throw new IOException("Ошибка при открытии файла метаданных", e);
        }

        try
        {
            var (term, votedFor) = InitializeFile(fileStream);
            return new MetadataFile(fileStream, term, votedFor);
        }
        catch (Exception)
        {
            fileStream.Dispose();
            throw;
        }
    }

    private static void FillFile(Term term, NodeId? votedFor, Span<byte> span)
    {
        var writer = new SpanBinaryWriter(span);
        writer.Write(Marker);
        writer.Write(CurrentVersion);
        writer.Write(term.Value);
        writer.Write(votedFor?.Id ?? NoVotedFor);
        var checkSum = Crc32CheckSum.Compute(span[sizeof(int)..^sizeof(uint)]);
        writer.Write(checkSum);
    }

    internal void SetupMetadataTest(Term initialTerm, NodeId? votedFor)
    {
        Span<byte> span = stackalloc byte[FileSize];
        FillFile(initialTerm, votedFor, span);
        _file.Seek(0, SeekOrigin.Begin);
        _file.Write(span);

        Term = initialTerm;
        VotedFor = votedFor;
    }
}