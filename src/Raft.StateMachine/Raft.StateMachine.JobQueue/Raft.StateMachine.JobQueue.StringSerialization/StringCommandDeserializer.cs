using System.Text;
using Raft.StateMachine.JobQueue.Requests;
using Raft.StateMachine.JobQueue.Serialization;
using Raft.StateMachine.JobQueue.StringSerialization.Commands;

namespace Raft.StateMachine.JobQueue.StringSerialization;

public class StringCommandDeserializer: ICommandDeserializer
{
    private readonly Encoding _encoding;

    public StringCommandDeserializer(Encoding encoding)
    {
        _encoding = encoding;
    }
    
    public ICommand Deserialize(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        
        try
        {
            var commandString = _encoding.GetString(payload);
            var tokens = commandString.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            switch (tokens)
            {
                case {Length: 0}:
                    return new WriteDirectResponseCommand("error: empty request string");
                default:
                    return DeserializeTokens(tokens);
            }
        }
        catch (DecoderFallbackException)
        { }
        catch (ArgumentException)
        { }
        
        return new ErrorResponseCommand("can not deserialize command");
    }

    private ICommand DeserializeTokens(string[] tokens)
    {
        var first = tokens[0];
        if (first.Equals("enqueue", StringComparison.InvariantCultureIgnoreCase))
        {
            return DeserializeEnqueueCommand(tokens);
        }

        if (first.Equals("dequeue", StringComparison.InvariantCultureIgnoreCase))
        {
            return DeserializeDequeueCommand(tokens);
        }

        if (first.Equals("count", StringComparison.InvariantCultureIgnoreCase))
        {
            return DeserializeGetCountCommand(tokens);
        }

        return new ErrorResponseCommand("unknown command");
    }

    private ICommand DeserializeGetCountCommand(string[] tokens)
    {
        return new GetCountCommand(GetCountRequest.Instance);
    }

    private ICommand DeserializeDequeueCommand(string[] tokens)
    {
        return new DequeueCommand(DequeueRequest.Instance);
    }

    private ICommand DeserializeEnqueueCommand(string[] tokens)
    {
        if (tokens.Length != 3)
        {
            return new ErrorResponseCommand($"invalid arguments count: expected 3 words, got {tokens.Length}");
        }
        
        try
        {
            var key = int.Parse(tokens[1]);
            var payload = _encoding.GetBytes(tokens[2]);
            return new EnqueueCommand(new EnqueueRequest(key, payload));
        }
        catch (FormatException)
        {
            return new ErrorResponseCommand("invalid key integer format");
        }
        catch (OverflowException)
        {
            return new ErrorResponseCommand("integer to big or small for integer");
        }
    }
}