using System.Buffers.Binary;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Commands.Submit;
using Consensus.Raft.Persistence;
using Consensus.Raft.Persistence.Log;
using Consensus.Raft.Persistence.Metadata;
using Consensus.Raft.Persistence.Snapshot;
using Consensus.Raft.Tests.Infrastructure;
using Moq;
using Serilog.Core;
using TaskFlux.Core;

namespace Consensus.Raft.Tests;

[Trait("Category", "Raft")]
public class LeaderStateTests
{
    private static readonly NodeId NodeId = new(1);
    private static readonly NodeId AnotherNodeId = new(NodeId.Id + 1);

    private class IntCommandSerializer : ICommandSerializer<int>
    {
        public byte[] Serialize(int value)
        {
            var result = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(result, value);
            return result;
        }

        public int Deserialize(byte[] payload)
        {
            return BinaryPrimitives.ReadInt32BigEndian(payload);
        }
    }

    private static readonly ICommandSerializer<int> CommandSerializer = new IntCommandSerializer();

    private RaftConsensusModule CreateLeaderNode(Term term,
                                                 NodeId? votedFor,
                                                 ITimer? heartbeatTimer = null,
                                                 IPeer[]? peers = null,
                                                 IStateMachine? stateMachine = null)
    {
        var peerGroup = new PeerGroup(peers ?? Array.Empty<IPeer>());
        heartbeatTimer ??= Helpers.NullTimer;
        stateMachine ??= Helpers.NullStateMachine;
        var facade = CreateFacade();
        var node = new RaftConsensusModule(NodeId, peerGroup, Logger.None, Helpers.NullTimer, heartbeatTimer,
            Helpers.NullBackgroundJobQueue, facade, Helpers.NullCommandQueue, stateMachine,
            CommandSerializer, Helpers.NullStateMachineFactory);
        node.SetStateTest(node.CreateLeaderState());
        return node;

        StoragePersistenceFacade CreateFacade()
        {
            var fs = Helpers.CreateFileSystem();
            var log = new FileLogStorage(fs.Log.Open(FileMode.Open));
            var metadata = new FileMetadataStorage(fs.Metadata.Open(FileMode.Open), term, votedFor);
            var snapshotStorage = new FileSystemSnapshotStorage(fs.Snapshot, fs.TemporaryDirectory);
            return new StoragePersistenceFacade(log, metadata, snapshotStorage);
        }
    }

    [Fact]
    public void RequestVote__СБолееВысокимТермом__ДолженПерейтиВFollower()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();
        using var node = CreateLeaderNode(term, null);

        var request = new RequestVoteRequest(AnotherNodeId, expectedTerm, LogEntryInfo.Tomb);
        var response = node.Handle(request);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.True(response.VoteGranted);
        Assert.Equal(expectedTerm, response.CurrentTerm);
        Assert.Equal(expectedTerm, node.CurrentTerm);
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(3, 2)]
    [InlineData(5, 1)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    public void RequestVote__СТермомМеньшеСвоего__ДолженОтветитьОтрицательно(
        int myTerm,
        int otherTerm)
    {
        var term = new Term(myTerm);

        using var node = CreateLeaderNode(term, null);

        var request = new RequestVoteRequest(CandidateId: AnotherNodeId, CandidateTerm: new(otherTerm),
            LastLogEntryInfo: node.PersistenceFacade.LastEntry);

        var response = node.Handle(request);

        Assert.False(response.VoteGranted);
        Assert.Equal(NodeRole.Leader, node.CurrentRole);
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(5, 1)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    [InlineData(3, 1)]
    [InlineData(3, 2)]
    [InlineData(100, 2)]
    [InlineData(100, 99)]
    [InlineData(1001, 1000)]
    public void AppendEntries__СТермомМеньшеСвоего__ДолженОтветитьОтрицательно(int myTerm, int otherTerm)
    {
        var term = new Term(myTerm);

        using var node = CreateLeaderNode(term, null);

        var request = AppendEntriesRequest.Heartbeat(new(otherTerm), node.PersistenceFacade.CommitIndex, AnotherNodeId,
            node.PersistenceFacade.LastEntry);

        var response = node.Handle(request);

        Assert.False(response.Success);
    }

    [Fact]
    public void AppendEntries__СБолееВысокимТермом__ДолженСтатьFollower()
    {
        var term = new Term(1);
        using var node = CreateLeaderNode(term, null);
        var expectedTerm = term.Increment();
        var request = AppendEntriesRequest.Heartbeat(expectedTerm, node.PersistenceFacade.CommitIndex,
            AnotherNodeId, node.PersistenceFacade.LastEntry);

        var response = node.Handle(request);

        Assert.True(response.Success);
        Assert.Equal(expectedTerm, response.Term);
        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.Equal(expectedTerm, node.CurrentTerm);
    }

    [Fact]
    public void ПриОтправкеHeartbeat__КогдаУзелОтветилОтрицательноИЕгоТермБольше__ДолженПерейтиВFollower()
    {
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Stop()).Verifiable();
            t.Setup(x => x.Start());
        });
        var peerTerm = term.Increment();
        var peer = new Mock<IPeer>();

        peer.SetupSequence(x =>
                 x.SendAppendEntriesAsync(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse(term, true))       // Первый вызов для Heartbeat
            .ReturnsAsync(new AppendEntriesResponse(peerTerm, false)); // Второй для нас

        using var node = CreateLeaderNode(term, null,
            heartbeatTimer: heartbeatTimer.Object,
            peers: new[] {peer.Object});

        // TODO: подконтрольный мне таймер для Heartbeat
        Thread.Sleep(1000);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    private static LogEntry IntDataEntry(Term term)
    {
        var data = new byte[Random.Shared.Next(0, 128)];
        Random.Shared.NextBytes(data);
        return new LogEntry(term, data);
    }

    private static LogEntry IntDataEntry(int term) => IntDataEntry(new Term(term));

    [Fact]
    public void AppendEntries__КогдаТермБольше__ДолженДобавитьЗаписиВЛог()
    {
        var term = new Term(3);
        using var node = CreateLeaderNode(term, null);
        var expectedTerm = term.Increment();
        var entries = new[] {IntDataEntry(1), IntDataEntry(2), IntDataEntry(2), IntDataEntry(3),};
        var request = new AppendEntriesRequest(expectedTerm, LogEntryInfo.Tomb.Index, AnotherNodeId, LogEntryInfo.Tomb,
            entries);
        var response = node.Handle(request);

        Assert.True(response.Success);
        Assert.Equal(expectedTerm, node.CurrentTerm);

        var actualEntries = node.PersistenceFacade.ReadLogBufferTest();
        Assert.Equal(entries, actualEntries);
    }

    private static readonly LogEntryEqualityComparer LogEntryComparer = LogEntryEqualityComparer.Instance;

    private static LogEntry IntLogEntry(Term term)
    {
        var buffer = new byte[4];
        Random.Shared.NextBytes(buffer);
        return new LogEntry(term, buffer);
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(2, 0)]
    [InlineData(5, 0)]
    [InlineData(5, 1)]
    [InlineData(5, 2)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    public void AppendEntries__КогдаТермБольше__ДолженЗакоммититьЗаписи(int bufferSize, int index)
    {
        var term = new Term(3);
        var expectedTerm = term.Increment();
        var nodeEntries = Enumerable.Range(0, bufferSize)
                                    .Select(_ => IntLogEntry(term))
                                    .ToArray();
        var (expectedFile, expectedBuffer) = nodeEntries.Split(index);
        using var node = CreateLeaderNode(term, null);
        node.PersistenceFacade.SetupBufferTest(nodeEntries);
        var request = new AppendEntriesRequest(expectedTerm, index, AnotherNodeId, LogEntryInfo.Tomb,
            Array.Empty<LogEntry>());
        var response = node.Handle(request);

        Assert.True(response.Success);
        Assert.Equal(expectedTerm, response.Term);

        var actualFile = node.PersistenceFacade.ReadLogFileTest();
        Assert.Equal(expectedFile, actualFile, LogEntryComparer);
        var actualBuffer = node.PersistenceFacade.ReadLogBufferTest();
        Assert.Equal(expectedBuffer, actualBuffer, LogEntryComparer);
    }

    private static SubmitRequest<int> CreateSubmitRequest(int value = 0) =>
        new(new CommandDescriptor<int>(value, false));

    [Fact(Timeout = 1000)]
    public void SubmitRequest__КогдаДругихУзловНет__ДолженОбработатьЗапрос()
    {
        var term = new Term(1);
        var mock = new Mock<IStateMachine>();
        var command = 1;
        var expectedResponse = 123;
        var request = CreateSubmitRequest(command);
        mock.Setup(x => x.Apply(It.Is<int>(y => y == command)))
            .Returns(expectedResponse)
            .Verifiable();
        using var node = CreateLeaderNode(term, null, peers: Array.Empty<IPeer>(), stateMachine: mock.Object);

        var response = node.Handle(request);

        Assert.Equal(expectedResponse, response.Response);
        var committedEntry = node.PersistenceFacade.ReadLogFileTest().Single();
        AssertCommandEqual(committedEntry, command);
        mock.Verify(x => x.Apply(It.Is<int>(y => y == command)), Times.Once());
    }

    private static void AssertCommandEqual(LogEntry entry, int expected)
    {
        Assert.Equal(expected, CommandSerializer.Deserialize(entry.Data));
    }

    [Fact(Timeout = 1000)]
    public void SubmitRequest__КогдаЕдинственныйДругойУзелОтветилУспехом__ДолженОбработатьЗапрос()
    {
        var term = new Term(1);
        var machine = new Mock<IStateMachine>();
        var command = 1;
        var expectedResponse = 123;
        var peer = new Mock<IPeer>();
        peer.Setup(x => x.SendAppendEntriesAsync(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse(term, true))
            .Verifiable();
        var request = CreateSubmitRequest(command);
        machine.Setup(x => x.Apply(It.Is<int>(y => y == command)))
               .Returns(expectedResponse)
               .Verifiable();

        using var node = CreateLeaderNode(term, null,
            peers: new[] {peer.Object},
            stateMachine: machine.Object);

        var response = node.Handle(request);

        Assert.Equal(expectedResponse, response.Response);
        var committedEntry = node.PersistenceFacade.ReadLogFileTest().Single();
        AssertCommandEqual(committedEntry, command);
        machine.Verify(x => x.Apply(It.Is<int>(y => y == command)), Times.Once());
    }

    // public void SubmitRequest__Когда
}