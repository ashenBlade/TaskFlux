using System.Net;
using TaskFlux.Commands;
using TaskFlux.Commands.Serialization;
using TaskFlux.Network.Client.Frontend.Exceptions;
using TaskFlux.Network.Requests;
using TaskFlux.Network.Requests.Authorization;
using TaskFlux.Network.Requests.Packets;
using TaskFlux.Node;

namespace TaskFlux.Network.Client.Frontend;

public class FrontendTaskFluxClient
{
    private readonly CommandSerializer _serializer;
    public ITaskFluxClient Client { get; }

    public FrontendTaskFluxClient(ITaskFluxClient client, CommandSerializer serializer)
    {
        _serializer = serializer;
        Client = client;
    }

    public async Task ConnectAsync(EndPoint endPoint, AuthorizationMethod method, CancellationToken token = default)
    {
        await Client.ConnectAsync(endPoint, token);
        try
        {
            await AuthorizeAsync(method, token);
            await BootstrapAsync(token);
        }
        catch (Exception)
        {
            await Client.DisconnectAsync(token);
            throw;
        }
    }

    private async Task BootstrapAsync(CancellationToken token)
    {
        var version = Constants.CurrentVersion;
        await Client.SendAsync(new BootstrapRequestPacket(version.Major, version.Minor, version.Build), token);
        var bootstrapResponse = await Client.ReceiveAsync(token);
        var bootstrapResponseVisitor = new BootstrapResponsePacketVisitor();
        bootstrapResponse.Accept(bootstrapResponseVisitor);
    }

    private class BootstrapResponsePacketVisitor : BaseResponsePacketVisitor
    {
        public BootstrapResponsePacketVisitor(): base(PacketType.BootstrapResponse)
        { }
        public override void Visit(BootstrapResponsePacket packet)
        {
            if (packet.TryGetError(out var reason))
            {
                BootstrapErrorResponseException.Throw(reason);
            }
        }
    }

    private async Task AuthorizeAsync(AuthorizationMethod method, CancellationToken token)
    {
        await Client.SendAsync(new AuthorizationRequestPacket(method), token);
        var authResponsePacket = await Client.ReceiveAsync(token);
        var authPacketVisitor = new AuthorizationResponsePacketVisitor();
        authResponsePacket.Accept(authPacketVisitor);
    }

    private class AuthorizationResponsePacketVisitor : BaseResponsePacketVisitor
    {
        public AuthorizationResponsePacketVisitor(): base(PacketType.AuthorizationResponse)
        { }
        
        public override void Visit(AuthorizationResponsePacket packet)
        {
            if (packet.TryGetError(out var reason))
            {
                AuthorizationFailedException.Throw(reason);
            }
        }
    }

    public async Task DisconnectAsync(CancellationToken token = default)
    {
        await Client.DisconnectAsync(token);
    }

    public async Task<Result> SendCommandAsync(Command command, CancellationToken token = default)
    {
        var commandPacket = new CommandRequestPacket(_serializer.Serialize(command));
        await Client.SendAsync(commandPacket, token);
        var responsePacket = await Client.ReceiveAsync(token);
        var visitor = new CommandResponsePacketProcessorVisitor(ResultSerializer.Instance);
        responsePacket.Accept(visitor);
        return visitor.Result;
    }

    private class CommandResponsePacketProcessorVisitor : BaseResponsePacketVisitor
    {
        public Result Result =>
            _result
         ?? throw new InvalidOperationException(
                "Обнаружена попытка получить доступ к неинициализированному значению результата операции");
        
        private Result? _result;
        private readonly ResultSerializer _serializer;
        public CommandResponsePacketProcessorVisitor(ResultSerializer serializer): base(PacketType.CommandResponse)
        {
            _serializer = serializer;
        }
        
        public override void Visit(CommandResponsePacket packet)
        {
            _result = _serializer.Deserialize(packet.Payload);
        }

    }
}