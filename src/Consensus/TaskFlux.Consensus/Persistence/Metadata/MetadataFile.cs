using System.Diagnostics;
using System.IO.Abstractions;
using TaskFlux.Core;
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

    private const long HeaderSize = DataStartPosition;

    private const long TermPosition = DataStartPosition + 0;
    private const long VotedForPosition = TermPosition + sizeof(int);

    /// <summary>
    /// Флаг указывающий на то, что голоса отдано не было.
    /// Записывается на месте Id узла
    /// </summary>
    private const int NoVotedFor = 0;

    /// <summary>
    /// Терм, чтобы использовать, если файл не был изначально инициализирован
    /// </summary>
    public Term Term { get; private set; }

    /// <summary>
    /// Голос, который будет использоваться, если файл не был изначально иницилазирован
    /// </summary>
    public NodeId? VotedFor { get; private set; }

    private readonly Stream _file;

    private MetadataFile(Stream file, Term term, NodeId? votedFor)
    {
        _file = file;
        VotedFor = votedFor;
        Term = term;
    }

    public void Update(Term term, NodeId? votedFor)
    {
        Debug.Assert(_file.Length > 0, "Файл должен быть инициализирован на момент вызова Update");

        const int size = sizeof(int)  // Маркер 
                       + sizeof(int)  // Версия
                       + sizeof(int)  // Терм
                       + sizeof(int); // Голос

        Span<byte> span = stackalloc byte[size];

        var writer = new SpanBinaryWriter(span);
        // Сначала записываем во внутренний буфер терм и голос,
        // потом быстро сбрасываем полученные данные на диск
        writer.Write(Marker);
        writer.Write(CurrentVersion);
        writer.Write(term.Value);
        writer.Write(votedFor is {Id: var value}
                         ? value
                         : NoVotedFor);

        _file.Position = 0;
        _file.Write(span);
        _file.Flush();

        Term = term;
        VotedFor = votedFor;
    }

    /// <summary>
    /// Полный размер файла.
    /// Файл может быть либо пустым (размер 0), либо этого размера.
    /// Другие значения указывают на беды с башкой
    /// </summary>
    private const int FileSize = sizeof(int)  // Маркер 
                               + sizeof(int)  // Версия
                               + sizeof(int)  // Терм
                               + sizeof(int); // Голос


    internal (Term, NodeId?) ReadStoredDataTest()
    {
        var reader = new StreamBinaryReader(_file);
        _file.Seek(0, SeekOrigin.Begin);

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

        var term = new Term(reader.ReadInt32());
        var votedFor = ( NodeId? ) ( reader.ReadInt32() switch
                                     {
                                         0         => null,
                                         var value => new NodeId(value)
                                     } );
        return ( term, votedFor );
    }

    private static (Term Term, NodeId? VotedFor) InitializeFile(FileSystemStream file)
    {
        if (file.Length == 0)
        {
            Span<byte> span = stackalloc byte[FileSize];

            var writer = new SpanBinaryWriter(span);
            // Сначала записываем во внутренний буфер терм и голос,
            // потом быстро сбрасываем полученные данные на диск
            writer.Write(Marker);
            writer.Write(CurrentVersion);
            writer.Write(DefaultTerm.Value);
            writer.Write(NoVotedFor); // DefaultVotedFor

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

        // Проверяем валидность данных на файле
        file.Seek(0, SeekOrigin.Begin);
        var reader = new StreamBinaryReader(file);
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

        var term = ReadTerm();
        var votedFor = ReadVotedFor();

        return ( term, votedFor );

        Term ReadTerm()
        {
            int? value = null;
            try
            {
                var termValue = reader.ReadInt32();
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

        NodeId? ReadVotedFor()
        {
            int? value = null;
            try
            {
                var read = reader.ReadInt32();
                value = read;
                return read switch
                       {
                           0     => null,
                           var v => new NodeId(v)
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

    internal void SetupMetadataTest(Term initialTerm, NodeId? votedFor)
    {
        _file.Seek(DataStartPosition, SeekOrigin.Begin);
        var writer = new StreamBinaryWriter(_file);
        writer.Write(initialTerm.Value);
        writer.Write(votedFor is {Id: var id}
                         ? id
                         : NoVotedFor);
        Term = initialTerm;
        VotedFor = votedFor;
    }
}