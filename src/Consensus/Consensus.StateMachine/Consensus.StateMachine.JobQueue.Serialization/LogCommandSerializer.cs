using System.Diagnostics;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using Consensus.Core;
using TaskFlux.Requests;
using TaskFlux.Requests.Serialization;

namespace Consensus.StateMachine.JobQueue.Serialization;

/// <summary>
/// 
/// </summary>
public class LogCommandSerializer: ICommandSerializer<IRequest>
{
    private readonly ISerializer<IRequest> _serializer;

    public LogCommandSerializer(ISerializer<IRequest> serializer)
    {
        _serializer = serializer;
    }

    public byte[] Serialize(IRequest command)
    {
        var estimator = new RequestSizeEstimatorVisitor();
        command.Accept(estimator);
        var buffer = new byte[estimator.EstimatedSize];
        var memory = new MemoryStream(buffer);
        _serializer.Serialize(command, new BinaryWriter(memory));
        
        Debug.Assert(
            estimator.EstimatedSize == memory.Position, 
            "estimator.EstimatedSize == memory.Position",
            "Размер выделенной памяти должен быть равен количеству записанной");
        
        return buffer;
    }

    public IRequest Deserialize(byte[] payload)
    {
        return _serializer.Deserialize(new BinaryReader(new MemoryStream(payload)));
    }
}