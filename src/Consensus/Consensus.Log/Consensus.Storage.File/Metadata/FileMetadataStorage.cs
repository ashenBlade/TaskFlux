using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.Arm;
using System.Text;
using Consensus.Core;

namespace Consensus.Storage.File;

public class FileMetadataStorage: IMetadataStorage
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
    /// Терм, чтобы использовать, если файл не был изначально инициализирован
    /// </summary>
    public Term InitialTerm { get; }
    
    /// <summary>
    /// Голос, который будет использоваться, если файл не был изначально иницилазирован
    /// </summary>
    public NodeId? InitialVotedFor { get; }
    private readonly Stream _file;
    private readonly BinaryWriter _writer;
    private readonly BinaryReader _reader;

    private volatile bool _initialized;
    
    public FileMetadataStorage(Stream file, Term initialTerm, NodeId? initialVotedFor)
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
        InitialTerm = initialTerm;
        InitialVotedFor = initialVotedFor;
        _writer = new BinaryWriter(_file, Encoding.UTF8, true);
        _reader = new BinaryReader(_file, Encoding.UTF8, true);
    }

    public Term ReadTerm()
    {
        CheckInitialized();
        
        _file.Seek(TermPosition, SeekOrigin.Begin);
        var term = _reader.ReadInt32();
        return new Term(term);
    }
    
    public NodeId? ReadVotedFor()
    {
        CheckInitialized();

        _file.Seek(VotedForPosition, SeekOrigin.Begin);
        var hasValue = _reader.ReadBoolean();
        if (hasValue)
        {
            var votedFor = _reader.ReadInt32();
            return new NodeId(votedFor);
        }
        
        return null;
    }

    public void Update(Term term, NodeId? votedFor)
    {
        CheckInitialized();

        _file.Seek(DataStartPosition, SeekOrigin.Begin);
        WriteAll(term, votedFor, _writer);
        _writer.Flush();
    }

    private void CheckInitialized()
    {
        if (!_initialized)
        {
            Initialize();
        }
    }
    
    private void Initialize()
    {
        if (_file.Length == 0)
        {
            _file.Seek(0, SeekOrigin.Begin);
            _writer.Write(Marker);
            _writer.Write(CurrentVersion);
            
            WriteAll(InitialTerm, InitialVotedFor, _writer);
            
            _writer.Flush();
            _initialized = true;
            return;
        }

        if (_file.Length < HeaderSize)
        {
            throw new InvalidDataException("Размер файла меньше размера заголовка");
        }

        _file.Seek(0, SeekOrigin.Begin);

        var marker = _reader.ReadInt32();
        if (marker != Marker)
        {
            throw new InvalidDataException(
                $"Хранившийся в файле маркер не равен требуемому. Считанный: {marker}. Требуемый: {Marker}");
        }

        var version = _reader.ReadInt32();
        if (CurrentVersion < version)
        {
            throw new InvalidDataException(
                $"Хранившаяся в файле версия больше текущей. Считанная версия: {version}. Текущая версия: {CurrentVersion}");
        }

        
        if (_file.Position == _file.Length)
        {
            // Дальше нет данных, поэтому инициализируем начальными 
            WriteAll(InitialTerm, InitialVotedFor, _writer);
            _initialized = true;
            return;
        }
        
        // Пропускаем чтение терма
        _writer.Seek(sizeof(int), SeekOrigin.Current);

        if (_file.Position == _file.Length)
        {
            WriteVote(InitialVotedFor, _writer);   
        }
        
        _initialized = true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteAll(Term term, NodeId? votedFor, BinaryWriter writer)
    {
        writer.Write(term.Value);
        WriteVote(votedFor, writer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteVote(NodeId? votedFor, BinaryWriter writer)
    {
        if (votedFor is {Value: var vote})
        {
            writer.Write(true);
            writer.Write(vote);
        }
        else
        {
            writer.Write(false);
            writer.Write(NodeId.None.Value);
        }
    }

    /// <summary>
    /// Создать новый <see cref="FileMetadataStorage"/> и тут же его иницилизировать
    /// </summary>
    /// <param name="stream">Поток хранимых данных. На проде - это файл (<see cref="FileStream"/>)</param>
    /// <param name="defaultTerm">Терм по умолчанию, если исходный файл был пуст/не иницилизирован</param>
    /// <param name="defaultVotedFor">Отданный голос по умолчанию, если исходный файл был пуст/не иницилизирован</param>
    /// <returns>Новый, инициализированный <see cref="FileMetadataStorage"/></returns>
    /// <exception cref="ArgumentException"><paramref name="stream"/> - не поддерживает чтение, запись или позиционирование</exception>
    /// <exception cref="InvalidDataException">
    /// Обнаружены ошибки во время инициализации файла (потока) данных: <br/>
    ///    - Поток не пуст и при этом его размер меньше минимального (размер заголовка) <br/> 
    ///    - Полученное магическое число не соответствует требуемому <br/>
    ///    - Указанная в файле версия несовместима с текущей <br/>\
    /// </exception>
    /// <exception cref="IOException">Во время чтения данных произошла ошибка</exception>
    public static FileMetadataStorage Initialize(Stream stream, Term defaultTerm, NodeId? defaultVotedFor)
    {
        var storage = new FileMetadataStorage(stream, defaultTerm, defaultVotedFor);
        storage.Initialize();
        return storage;
    }
}