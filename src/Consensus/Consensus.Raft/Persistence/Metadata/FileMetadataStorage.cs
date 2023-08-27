using System.Diagnostics;
using TaskFlux.Core;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Raft.Persistence.Metadata;

public class FileMetadataStorage : IMetadataStorage
{
    private const int Marker = Constants.Marker;

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

    /// <summary>
    /// Конструктор по умолчанию для хранилища метаданных.
    /// Внутри происходит логика инициализации файла и некоторые его проверки.
    /// </summary>
    /// <param name="file">Поток файла метаданных. Файл может быть либо пустым, либо сразу инициализированным (фиксированный размер)</param>
    /// <param name="defaultTerm">Терм, который нужно использовать, если файл был пуст</param>
    /// <param name="defaultVotedFor">Голос, который нужно использовать по умолчанию, если файл был пуст</param>
    /// <exception cref="ArgumentException">Переданный поток не поддерживает чтение и/или запись и/или позиционирование</exception>
    /// <exception cref="InvalidDataException">
    /// Обнаружены ошибки во время инициализации файла (потока) данных: <br/>
    ///    - Поток не пуст и при этом его размер меньше минимального (размер заголовка) <br/> 
    ///    - Полученное магическое число не соответствует требуемому <br/>
    ///    - Указанная в файле версия несовместима с текущей <br/>\
    /// </exception>
    /// <exception cref="IOException">Во время чтения данных произошла ошибка</exception>
    public FileMetadataStorage(Stream file, Term defaultTerm, NodeId? defaultVotedFor)
    {
        if (!file.CanRead)
        {
            throw new ArgumentException("Переданный поток не поддерживает чтение", nameof(file));
        }

        if (!file.CanSeek)
        {
            throw new ArgumentException("Переданный поток не поддерживает позиционирование", nameof(file));
        }

        if (!file.CanWrite)
        {
            throw new ArgumentException("Переданный поток не поддерживает запись", nameof(file));
        }

        _file = file;
        InitializeAtStart(defaultTerm, defaultVotedFor);

        // 1. StreamBinaryWriter/Reader
        // 2. Кэшировать терм и голос
        // 3. Инициализация сразу в конструкторе
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
        writer.Write(votedFor is {Value: var value}
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

    private void InitializeAtStart(Term defaultTerm, NodeId? defaultVotedFor)
    {
        if (_file.Length == 0)
        {
            Span<byte> span = stackalloc byte[FileSize];

            var writer = new SpanBinaryWriter(span);
            // Сначала записываем во внутренний буфер терм и голос,
            // потом быстро сбрасываем полученные данные на диск
            writer.Write(Marker);
            writer.Write(CurrentVersion);
            writer.Write(defaultTerm.Value);
            writer.Write(defaultVotedFor is {Value: var value}
                             ? value
                             : NoVotedFor);

            _file.Seek(0, SeekOrigin.Begin);
            _file.Write(span);
            _file.Flush();

            Term = defaultTerm;
            VotedFor = defaultVotedFor;

            return;
        }

        if (_file.Length < FileSize)
        {
            // Пока указываю минимальную длину вместо фиксированной.
            // В будующем, возможно, появятся другие версии.
            // Надо сохранить совместимость.
            throw new InvalidDataException(
                $"Файл может быть либо пустым, либо не меньше фиксированной длины. Размер файла: {_file.Length}. Минимальный размер: {FileSize}");
        }

        // Проверяем валидность данных на файле
        _file.Seek(0, SeekOrigin.Begin);
        var reader = new StreamBinaryReader(_file);
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
        var readVotedFor = ReadVotedFor();

        Term = term;
        VotedFor = readVotedFor;

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
}