using System.Buffers.Binary;
using Moq;
using Serilog.Core;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Consensus.Tests.Infrastructure;
using TaskFlux.Consensus.Tests.Stubs;
using TaskFlux.Core;
using IApplication = TaskFlux.Consensus.Tests.Infrastructure.IApplication;

namespace TaskFlux.Consensus.Tests;

[Trait("Category", "Raft")]
public class LeaderStateTests
{
    private static readonly NodeId NodeId = new(1);
    private static readonly NodeId AnotherNodeId = new(NodeId.Id + 1);

    private class IntDeltaExtractor : IDeltaExtractor<int>
    {
        public bool TryGetDelta(int response, out byte[] delta)
        {
            delta = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(delta, response);
            return true;
        }
    }

    private static readonly IDeltaExtractor<int> DeltaExtractor = new IntDeltaExtractor();

    private Mock<IPersistence> _mockPersistence = null!;

    private RaftConsensusModule CreateLeaderNode(Term term,
                                                 NodeId? votedFor,
                                                 ITimer? heartbeatTimer = null,
                                                 IEnumerable<IPeer>? peers = null,
                                                 IApplication? application = null,
                                                 IBackgroundJobQueue? jobQueue = null,
                                                 Action<Mock<IPersistence>>? persistenceSetup = null)
    {
        var peerGroup = new PeerGroup(peers switch
                                      {
                                          null      => Array.Empty<IPeer>(),
                                          IPeer[] p => p,
                                          not null  => peers.ToArray(),
                                      });
        var timerFactory = heartbeatTimer is null
                               ? Helpers.NullTimerFactory
                               : new ConstantTimerFactory(heartbeatTimer);
        var persistence = new Mock<IPersistence>(MockBehavior.Strict).Apply(m =>
        {
            m.SetupGet(p => p.CurrentTerm).Returns(term);
            m.SetupGet(p => p.VotedFor).Returns(votedFor);
            // Убираем ошибки при восстановлении состояния приложения при инициализации, если надо переопределим
            m.Setup(p => p.TryGetSnapshot(out It.Ref<ISnapshot>.IsAny, out It.Ref<LogEntryInfo>.IsAny)).Returns(false);
            m.Setup(p => p.ReadDeltaFromPreviousSnapshot()).Returns(Array.Empty<byte[]>());

            // Это для инициализации обработчиков
            m.SetupGet(p => p.LastEntry).Returns(LogEntryInfo.Tomb);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);

            persistenceSetup?.Invoke(m);
        });
        _mockPersistence = persistence;
        var factory = new Mock<IApplicationFactory<int, int>>().Apply(m =>
        {
            m.Setup(x => x.Restore(It.IsAny<ISnapshot?>(), It.IsAny<IEnumerable<byte[]>>()))
             .Returns(application ?? Helpers.NullApplication);
        });

        jobQueue ??= Helpers.NullBackgroundJobQueue;

        var node = new RaftConsensusModule(NodeId,
            peerGroup, Logger.None,
            timerFactory,
            jobQueue,
            persistence.Object,
            DeltaExtractor,
            factory.Object);
        node.SetStateTest(node.CreateLeaderState());
        return node;
    }

    [Fact]
    public void RequestVote__КогдаТермВЗапросеБольшеИЛогНеАктуальный__ДолженСтатьПоследователем()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.Setup(p => p.IsUpToDate(It.IsAny<LogEntryInfo>())).Returns(false);
        });

        _ = node.Handle(new RequestVoteRequest(AnotherNodeId, expectedTerm, LogEntryInfo.Tomb));

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void RequestVote__КогдаТермВЗапросеБольшеИЛогАктуальный__ДолженСтатьПоследователем()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.Setup(p => p.IsUpToDate(It.IsAny<LogEntryInfo>())).Returns(true);
        });

        _ = node.Handle(new RequestVoteRequest(AnotherNodeId, expectedTerm, LogEntryInfo.Tomb));

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void RequestVote__КогдаТермВЗапросеБольшеИЛогАктуальный__ДолженОтдатьГолосЗаУзел()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();
        var candidateId = AnotherNodeId;
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.Setup(p => p.IsUpToDate(It.IsAny<LogEntryInfo>())).Returns(true);
        });

        var response = node.Handle(new RequestVoteRequest(candidateId, expectedTerm, LogEntryInfo.Tomb));

        Assert.True(response.VoteGranted);
    }

    [Fact]
    public void RequestVote__КогдаТермВЗапросеБольшеИЛогАктуальный__ДолженОбновитьТерм()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();
        var candidateId = AnotherNodeId;
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(expectedTerm, It.IsAny<NodeId?>())).Verifiable();
            m.Setup(p => p.IsUpToDate(It.IsAny<LogEntryInfo>())).Returns(true);
        });

        _ = node.Handle(new RequestVoteRequest(candidateId, expectedTerm, LogEntryInfo.Tomb));

        _mockPersistence.Verify(p => p.UpdateState(expectedTerm, It.IsAny<NodeId?>()), Times.Once());
    }

    [Fact]
    public void RequestVote__КогдаТермВЗапросеБольшеИЛогАктуальный__ДолженВыставитьVotedForВIdКандидата()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();
        var candidateId = AnotherNodeId;
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), candidateId)).Verifiable();
            m.Setup(p => p.IsUpToDate(It.IsAny<LogEntryInfo>())).Returns(true);
        });

        _ = node.Handle(new RequestVoteRequest(candidateId, expectedTerm, LogEntryInfo.Tomb));

        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), candidateId), Times.Once());
    }

    [Fact]
    public void RequestVote__КогдаТермВЗапросеБольшеИЛогКонфликтует__ДолженСтатьПоследователем()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.Setup(p => p.IsUpToDate(It.IsAny<LogEntryInfo>())).Returns(false);
        });

        _ = node.Handle(new RequestVoteRequest(AnotherNodeId, expectedTerm, LogEntryInfo.Tomb));

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(3, 2)]
    [InlineData(5, 1)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    public void RequestVote__КогдаТермВЗапросеМеньшеМоего__ДолженОтветитьОтрицательно(
        int myTerm,
        int otherTerm)
    {
        var term = new Term(myTerm);

        using var node = CreateLeaderNode(term, null);

        var response = node.Handle(new RequestVoteRequest(AnotherNodeId, new(otherTerm), LogEntryInfo.Tomb));

        Assert.False(response.VoteGranted);
        Assert.Equal(NodeRole.Leader, node.CurrentRole);
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеНоЛогОтстает__НеДолженОтдатьГолос()
    {
        var term = new Term(1);
        var lastLogEntryInfo = new LogEntryInfo(term, 2);

        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(false);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
        });

        var greaterTerm = term.Increment();

        var request = new RequestVoteRequest(AnotherNodeId, greaterTerm, lastLogEntryInfo);
        var response = node.Handle(request);

        Assert.False(response.VoteGranted);
        Assert.Equal(greaterTerm, response.CurrentTerm);
    }

    [Fact]
    public void RequestVote__КогдаТермБольше__ДолженВыставитьТермИзЗапроса()
    {
        var term = new Term(1);
        var greaterTerm = term.Increment();

        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.Is<Term>(t => t == greaterTerm), It.IsAny<NodeId?>()));
            m.Setup(p => p.IsUpToDate(It.IsAny<LogEntryInfo>())).Returns(true);
        });

        _ = node.Handle(new RequestVoteRequest(AnotherNodeId, greaterTerm, new LogEntryInfo(term, 2)));
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеНоЛогОтстает__ДолженСтатьПоследователем()
    {
        var term = new Term(1);
        var lastLogEntryInfo = new LogEntryInfo(term, 2);

        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.Setup(p => p.IsUpToDate(It.IsAny<LogEntryInfo>())).Returns(false);
        });
        var greaterTerm = term.Increment();

        _ = node.Handle(new RequestVoteRequest(AnotherNodeId, greaterTerm, lastLogEntryInfo));

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеИЛогБолееАктуальный__ДолженСтатьПоследователем()
    {
        var term = new Term(1);
        var greaterTerm = term.Increment();
        var lastLogEntryInfo = new LogEntryInfo(greaterTerm, 4);
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
        });

        _ = node.Handle(new RequestVoteRequest(AnotherNodeId, greaterTerm, lastLogEntryInfo));

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеИЛогАктуальный__ДолженОтдатьГолос()
    {
        var term = new Term(1);
        var greaterTerm = term.Increment();
        var lastLogEntryInfo = new LogEntryInfo(greaterTerm, 4);
        var candidateId = AnotherNodeId;
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.Is<NodeId?>(id => id == candidateId)));
        });

        var response = node.Handle(new RequestVoteRequest(candidateId, greaterTerm, lastLogEntryInfo));

        Assert.True(response.VoteGranted);
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеИЛогАктуальный__ДолженОбновитьТерм()
    {
        var term = new Term(1);
        var greaterTerm = term.Increment();
        var lastLogEntryInfo = new LogEntryInfo(greaterTerm, 4);
        var candidateId = AnotherNodeId;
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
            m.Setup(p => p.UpdateState(greaterTerm, It.IsAny<NodeId?>())).Verifiable();
        });

        _ = node.Handle(new RequestVoteRequest(candidateId, greaterTerm, lastLogEntryInfo));

        _mockPersistence.Verify(p => p.UpdateState(greaterTerm, It.IsAny<NodeId?>()), Times.Once());
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеИЛогАктуальный__ДолженВернутьВОтветеНовыйТерм()
    {
        var term = new Term(1);
        var greaterTerm = term.Increment();
        var lastLogEntryInfo = new LogEntryInfo(greaterTerm, 4);
        var candidateId = AnotherNodeId;
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
            m.Setup(p => p.UpdateState(greaterTerm, It.IsAny<NodeId?>()));
        });

        var response = node.Handle(new RequestVoteRequest(candidateId, greaterTerm, lastLogEntryInfo));

        Assert.Equal(greaterTerm, response.CurrentTerm);
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеИЛогАктуальный__ДолженВыставитьVotedForВIdКандидата()
    {
        var term = new Term(1);
        var greaterTerm = term.Increment();
        var lastLogEntryInfo = new LogEntryInfo(greaterTerm, 4);
        var candidateId = AnotherNodeId;
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.IsUpToDate(lastLogEntryInfo)).Returns(true);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), candidateId)).Verifiable();
        });

        _ = node.Handle(new RequestVoteRequest(candidateId, greaterTerm, lastLogEntryInfo));

        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), candidateId), Times.Once());
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
    public void AppendEntries__КогдаТермВЗапросеМеньшеМоего__ДолженОтветитьОтрицательно(int myTerm, int otherTerm)
    {
        var term = new Term(myTerm);
        var prevLogEntryInfo = LogEntryInfo.Tomb;

        using var node = CreateLeaderNode(term, null);

        var response = node.Handle(new AppendEntriesRequest(new(otherTerm), Lsn.Tomb, AnotherNodeId, prevLogEntryInfo,
            new[] {new LogEntry(1, [123, 44, 12])}));

        Assert.False(response.Success);
    }

    [Fact]
    public void AppendEntries__КогдаПереданныйТермБольше__ДолженСтатьFollower()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();

        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.Setup(p => p.InsertRange(It.IsAny<IReadOnlyList<LogEntry>>(), It.IsAny<Lsn>()));
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        node.Handle(new AppendEntriesRequest(expectedTerm, Lsn.Tomb, AnotherNodeId, LogEntryInfo.Tomb,
            Array.Empty<LogEntry>()));

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void AppendEntries__КогдаПереданныйТермБольше__ДолженВыставитьТермКакВЗапросе()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();

        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.Is<Term>(t => t == expectedTerm), It.IsAny<NodeId?>()));
            m.Setup(p => p.InsertRange(It.IsAny<IReadOnlyList<LogEntry>>(), It.IsAny<Lsn>()));
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        node.Handle(new AppendEntriesRequest(expectedTerm, Lsn.Tomb, AnotherNodeId, LogEntryInfo.Tomb,
            Array.Empty<LogEntry>()));
    }

    [Fact]
    public void AppendEntries__КогдаПереданныйТермБольше__ДолженВыставитьVotedForВNull()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();

        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.Is<NodeId?>(id => id == null)));
            m.Setup(p => p.InsertRange(It.IsAny<IReadOnlyList<LogEntry>>(), It.IsAny<Lsn>()));
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        node.Handle(new AppendEntriesRequest(expectedTerm, Lsn.Tomb, AnotherNodeId, LogEntryInfo.Tomb,
            Array.Empty<LogEntry>()));
    }

    [Fact]
    public void AppendEntries__КогдаПереданныйТермБольшеИЛогСовпадает__ДолженОтветитьПоложительно()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();

        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.Is<NodeId?>(id => id == null)));
            m.Setup(p => p.InsertRange(It.IsAny<IReadOnlyList<LogEntry>>(), It.IsAny<Lsn>()));
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        var response = node.Handle(new AppendEntriesRequest(expectedTerm, Lsn.Tomb, AnotherNodeId, LogEntryInfo.Tomb,
            Array.Empty<LogEntry>()));

        Assert.True(response.Success);
    }

    [Fact]
    public void AppendEntries__КогдаТермВЗапросеБольшеНоЛогНеСовпадает__ДолженОтветитьОтрицательно()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();

        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.Is<NodeId?>(id => id == null)));
            m.Setup(p => p.InsertRange(It.IsAny<IReadOnlyList<LogEntry>>(), It.IsAny<Lsn>()));
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(false);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        var response = node.Handle(new AppendEntriesRequest(expectedTerm, Lsn.Tomb, AnotherNodeId, LogEntryInfo.Tomb,
            Array.Empty<LogEntry>()));

        Assert.False(response.Success);
    }

    [Fact]
    public void AppendEntries__КогдаТермВЗапросеРавенТекущему__ДолженВернутьFalse()
    {
        var term = new Term(1);
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.Is<NodeId?>(id => id == null)));
            m.Setup(p => p.InsertRange(It.IsAny<IReadOnlyList<LogEntry>>(), It.IsAny<Lsn>()));
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(false);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        var response = node.Handle(new AppendEntriesRequest(term, Lsn.Tomb, AnotherNodeId, LogEntryInfo.Tomb,
            Array.Empty<LogEntry>()));

        Assert.False(response.Success);
    }

    [Fact]
    public void Репликация__КогдаЛогДругогоУзлаБылПуст__ДолженСинхронизироватьЛог()
    {
        var term = new Term(4);
        var heartbeatTimer = new Mock<ITimer>().Apply(m =>
        {
            m.Setup(x => x.Schedule())
             .Verifiable();
            m.Setup(x => x.Stop())
             .Verifiable();
        });

        var peer = CreateDefaultPeer();
        var sentLogEntries = Array.Empty<LogEntry>();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns((AppendEntriesRequest request, CancellationToken _) =>
             {
                 // Откатываемся до момента начала лога (полностью пуст)
                 if (request.PrevLogEntryInfo.IsTomb)
                 {
                     sentLogEntries = request.Entries.ToArray();
                     return AppendEntriesResponse.Ok(request.Term);
                 }

                 return AppendEntriesResponse.Fail(request.Term);
             });
        var queue = new TaskBackgroundJobQueue();
        var allLogEntries = new[]
        {
            IntLogEntry(1), // 0
            IntLogEntry(2), // 1
            IntLogEntry(3), // 2
            IntLogEntry(4), // 3
        };
        var lastEntry = new LogEntryInfo(term, 3);
        using var node = CreateLeaderNode(term, null, heartbeatTimer: heartbeatTimer.Object, peers: new[] {peer.Object},
            jobQueue: queue, persistenceSetup: m =>
            {
                m.Setup(p => p.TryGetFrom(It.IsAny<Lsn>(), out It.Ref<IReadOnlyList<LogEntry>>.IsAny,
                      out It.Ref<LogEntryInfo>.IsAny))
                 .Callback((Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevEntryInfo) =>
                  {
                      entries = allLogEntries.Skip(( int ) index).ToArray();
                      prevEntryInfo = new LogEntryInfo(term, index - 1);
                  })
                 .Returns(true);
                m.Setup(p => p.GetEntryInfo(It.IsAny<Lsn>())).Returns((Lsn lsn) => new LogEntryInfo(term, lsn));
                m.SetupGet(p => p.LastEntry).Returns(lastEntry);
                m.SetupGet(p => p.CommitIndex).Returns(lastEntry.Index);
            });

        heartbeatTimer.Raise(x => x.Timeout += null);

        Assert.Equal(allLogEntries, sentLogEntries, Comparer);
    }

    [Fact]
    public void Репликация__КогдаУзел1ИБылНеПолностьюПуст__ДолженСинхронизироватьЛог()
    {
        var term = new Term(4);
        var heartbeatTimer = new Mock<ITimer>().Apply(m =>
        {
            m.Setup(x => x.Schedule())
             .Verifiable();
            m.Setup(x => x.Stop())
             .Verifiable();
        });

        var peer = CreateDefaultPeer();
        var finalReturnedLogEntries = Array.Empty<LogEntry>();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns((AppendEntriesRequest request, CancellationToken _) =>
             {
                 if (request.PrevLogEntryInfo.Index == 0) // Есть только 1 запись
                 {
                     finalReturnedLogEntries = request.Entries.ToArray();
                     return AppendEntriesResponse.Ok(request.Term);
                 }

                 return AppendEntriesResponse.Fail(request.Term);
             });
        var queue = new TaskBackgroundJobQueue();
        var allLogEntries = new[]
        {
            IntLogEntry(1), // 0
            IntLogEntry(2), // 1
            IntLogEntry(3), // 2
            IntLogEntry(4), // 3
        };
        var lastEntryInfo = new LogEntryInfo(term, 3);
        var expectedLogEntries = allLogEntries[1..]; // Была первая запись
        using var node = CreateLeaderNode(term, null, heartbeatTimer: heartbeatTimer.Object, peers: new[] {peer.Object},
            jobQueue: queue, persistenceSetup: m =>
            {
                m.Setup(p => p.TryGetFrom(It.IsAny<Lsn>(), out It.Ref<IReadOnlyList<LogEntry>>.IsAny,
                      out It.Ref<LogEntryInfo>.IsAny))
                 .Callback((Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevEntryInfo) =>
                  {
                      entries = allLogEntries.Skip(( int ) index).ToArray();
                      prevEntryInfo = new LogEntryInfo(term, index - 1);
                  })
                 .Returns(true);
                m.Setup(p => p.GetEntryInfo(It.IsAny<Lsn>())).Returns((Lsn lsn) => new LogEntryInfo(term, lsn));
                m.SetupGet(p => p.LastEntry).Returns(lastEntryInfo);
                m.Setup(p => p.Commit(It.IsAny<Lsn>()));
                m.SetupGet(p => p.CommitIndex).Returns(lastEntryInfo.Index);
            });

        heartbeatTimer.Raise(x => x.Timeout += null);

        Assert.Equal(expectedLogEntries, finalReturnedLogEntries, Comparer);
    }

    [Fact]
    public void Репликация__КогдаЕстьКластерИз3УзловИЧастьУзловНеДоКонцаРеплицирована__ДолженСинхронизироватьЛогиУзлов()
    {
        var term = new Term(4);
        var heartbeatTimer = new Mock<ITimer>().Apply(m =>
        {
            m.Setup(x => x.Schedule())
             .Verifiable();
            m.Setup(x => x.Stop())
             .Verifiable();
        });

        var allLogEntries = new[]
        {
            IntLogEntry(1), // 0
            IntLogEntry(1), // 1
            IntLogEntry(2), // 2
            IntLogEntry(3), // 3
            IntLogEntry(4), // 4
            IntLogEntry(4), // 5
        };

        // Лог первого узла полностью пуст
        var first = CreateStubPeer(Lsn.Tomb, allLogEntries);
        // Второй узел реплицирован до 2 индекса включительно
        var second = CreateStubPeer(new Lsn(2), allLogEntries.Skip(3));

        var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null,
            heartbeatTimer: heartbeatTimer.Object,
            peers: new[] {first.Object, second.Object},
            jobQueue: queue, persistenceSetup: m =>
            {
                m.Setup(p => p.TryGetFrom(It.IsAny<Lsn>(), out It.Ref<IReadOnlyList<LogEntry>>.IsAny,
                      out It.Ref<LogEntryInfo>.IsAny))
                 .Callback((Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevEntryInfo) =>
                  {
                      entries = allLogEntries.Skip(( int ) index).ToArray();
                      prevEntryInfo = new LogEntryInfo(term, index - 1);
                  })
                 .Returns(true);
                m.Setup(p => p.GetEntryInfo(It.IsAny<Lsn>()))
                 .Returns((Lsn lsn) => new LogEntryInfo(allLogEntries[( int ) lsn].Term, lsn));
                m.Setup(p => p.Commit(It.IsAny<Lsn>()));
                m.Setup(p => p.CommitIndex).Returns(new Lsn(allLogEntries.Length - 1));
                m.SetupGet(p => p.LastEntry)
                 .Returns(new LogEntryInfo(allLogEntries[^1].Term, allLogEntries.Length - 1));
            });

        heartbeatTimer.Raise(x => x.Timeout += null);

        return;

        static Mock<IPeer> CreateStubPeer(
            Lsn logStartIndex,
            IEnumerable<LogEntry> expectedEntries)
        {
            var completed = false;
            var peer = CreateDefaultPeer();
            peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
                .Returns((AppendEntriesRequest request, CancellationToken _) =>
                 {
                     // Откатываемся до момента начала лога (полностью пуст)
                     if (request.PrevLogEntryInfo.Index == logStartIndex)
                     {
                         if (completed)
                         {
                             throw new InvalidOperationException("Уже был отправлен запрос с требуемым логом");
                         }

                         completed = true;
                         if (!request.Entries.SequenceEqual(expectedEntries, Comparer))
                         {
                         }

                         return AppendEntriesResponse.Ok(request.Term);
                     }

                     // Откатываемся до момента, пока не получим -1 (самое начало лога)
                     return AppendEntriesResponse.Fail(request.Term);
                 });
            return peer;
        }
    }

    private static LogEntry IntLogEntry(Term term)
    {
        var data = new byte[Random.Shared.Next(0, 128)];
        Random.Shared.NextBytes(data);
        return new LogEntry(term, data);
    }

    private static readonly LogEntryEqualityComparer Comparer = LogEntryEqualityComparer.Instance;

    [Fact]
    public void AppendEntries__КогдаТермВЗапросеБольшеИИндексКоммитаБольше__ДолженОбновитьИндексКоммита()
    {
        var oldCommitIndex = 1;
        var newCommitIndex = new Lsn(4);
        var term = new Term(3);
        var greaterTerm = term.Increment();
        using var node = CreateLeaderNode(term, null, persistenceSetup: m =>
        {
            m.SetupGet(p => p.LastEntry).Returns(new LogEntryInfo(term, newCommitIndex + 1));
            m.Setup(p => p.Commit(It.Is<Lsn>(lsn => lsn == new Lsn(newCommitIndex)))).Verifiable();
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.SetupGet(p => p.CommitIndex).Returns(new Lsn(oldCommitIndex));
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.Setup(p => p.ShouldCreateSnapshot()).Returns(false);
        });

        var response = node.Handle(new AppendEntriesRequest(greaterTerm, newCommitIndex, AnotherNodeId,
            LogEntryInfo.Tomb, Array.Empty<LogEntry>()));

        Assert.True(response.Success);
        _mockPersistence.Verify(p => p.Commit(It.Is<Lsn>(lsn => lsn == new Lsn(newCommitIndex))), Times.Once());
    }

    [Fact]
    public void SubmitRequest__КогдаЕдинственныйВКластере__ДолженОбработатьЗапрос()
    {
        var term = new Term(1);
        var mockApplication = new Mock<IApplication>();
        var command = 1;
        var expectedResponse = 123;
        var request = command;
        mockApplication
           .Setup(x => x.Apply(It.Is<int>(y => y == command)))
           .Returns(expectedResponse)
           .Verifiable();
        var returnLsn = new Lsn(123);
        var lastEntryInfo = new LogEntryInfo(term, returnLsn - 1);
        using var node = CreateLeaderNode(term, null, peers: Array.Empty<IPeer>(), application: mockApplication.Object,
            persistenceSetup: m =>
            {
                m.Setup(p => p.Append(It.IsAny<LogEntry>())).Returns(returnLsn).Verifiable();
                m.Setup(p => p.Commit(It.Is<Lsn>(lsn => lsn == returnLsn))).Verifiable();
                m.SetupGet(p => p.LastEntry).Returns(lastEntryInfo);
                m.SetupGet(p => p.CommitIndex).Returns(lastEntryInfo.Index - 1);
                m.Setup(p => p.ShouldCreateSnapshot()).Returns(false);
            });

        var response = node.Handle(request);

        Assert.Equal(expectedResponse, response.Response);

        mockApplication.Verify(x => x.Apply(It.Is<int>(y => y == command)), Times.Once());

        _mockPersistence.Verify(p => p.Append(It.IsAny<LogEntry>()), Times.Once());
        _mockPersistence.Verify(p => p.Commit(It.Is<Lsn>(lsn => lsn == returnLsn)), Times.Once());
    }

    [Fact(Timeout = 1000)]
    public void SubmitRequest__КогдаЕдинственныйДругойУзелОтветилУспехом__ДолженОбработатьЗапрос()
    {
        var term = new Term(1);
        var machine = new Mock<IApplication>();
        var command = 1;
        var expectedResponse = 123;
        var peer = CreateDefaultPeer();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new AppendEntriesResponse(term, true))
            .Verifiable();
        machine.Setup(x => x.Apply(command))
               .Returns(expectedResponse)
               .Verifiable();
        var queue = new TaskBackgroundJobQueue();
        var returnLsn = new Lsn(166);
        var lastEntry = new LogEntryInfo(term, returnLsn - 1);
        using var node = CreateLeaderNode(term, null,
            peers: new[] {peer.Object},
            application: machine.Object,
            jobQueue: queue,
            persistenceSetup: m =>
            {
                m.Setup(p => p.Append(It.IsAny<LogEntry>())).Returns(returnLsn);
                m.Setup(p => p.Commit(returnLsn));
                m.Setup(p => p.TryGetFrom(It.IsAny<Lsn>(), out It.Ref<IReadOnlyList<LogEntry>>.IsAny,
                      out It.Ref<LogEntryInfo>.IsAny))
                 .Callback((Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevEntryInfo) =>
                  {
                      if (index != returnLsn)
                      {
                          throw new InvalidOperationException("Необходимо указать индекс вставленной записи");
                      }

                      entries = new[] {new LogEntry(term, "sample"u8.ToArray())};
                      prevEntryInfo = new LogEntryInfo(term, index - 1);
                  })
                 .Returns(true);
                m.Setup(p => p.GetEntryInfo(It.IsAny<Lsn>())).Returns((Lsn lsn) => new LogEntryInfo(term, lsn));
                m.SetupGet(p => p.LastEntry).Returns(lastEntry);
                m.SetupGet(p => p.CommitIndex).Returns(lastEntry.Index);
                m.Setup(p => p.ShouldCreateSnapshot()).Returns(false);
            });

        var response = node.Handle(command);

        Assert.Equal(expectedResponse, response.Response);
        machine.Verify(x => x.Apply(command), Times.Once());
        _mockPersistence.Verify(p => p.Append(It.IsAny<LogEntry>()), Times.Once());
        _mockPersistence.Verify(p => p.Commit(returnLsn), Times.Once());
    }

    [Fact]
    public void SubmitRequest__КогдаЕдинственныйДругойУзелОтветилУспехом__ДолженЗакоммититьКоманду()
    {
        var term = new Term(1);
        var machine = new Mock<IApplication>();
        var command = 1;
        var expectedResponse = 123;
        var peer = CreateDefaultPeer();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new AppendEntriesResponse(term, true))
            .Verifiable();
        machine.Setup(x => x.Apply(command))
               .Returns(expectedResponse)
               .Verifiable();
        var queue = new TaskBackgroundJobQueue();
        var returnLsn = new Lsn(166);
        var lastEntry = new LogEntryInfo(term, returnLsn - 1);
        using var node = CreateLeaderNode(term, null,
            peers: new[] {peer.Object},
            application: machine.Object,
            jobQueue: queue,
            persistenceSetup: m =>
            {
                m.Setup(p => p.Append(It.IsAny<LogEntry>())).Returns(returnLsn);
                m.Setup(p => p.Commit(returnLsn)).Verifiable();
                m.Setup(p => p.TryGetFrom(It.IsAny<Lsn>(), out It.Ref<IReadOnlyList<LogEntry>>.IsAny,
                      out It.Ref<LogEntryInfo>.IsAny))
                 .Callback((Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevEntryInfo) =>
                  {
                      if (index != returnLsn)
                      {
                          throw new InvalidOperationException("Необходимо указать индекс вставленной записи");
                      }

                      entries = new[] {new LogEntry(term, "sample"u8.ToArray())};
                      prevEntryInfo = new LogEntryInfo(term, index - 1);
                  })
                 .Returns(true);
                m.Setup(p => p.GetEntryInfo(It.IsAny<Lsn>())).Returns((Lsn lsn) => new LogEntryInfo(term, lsn));
                m.SetupGet(p => p.LastEntry).Returns(lastEntry);
                m.SetupGet(p => p.CommitIndex).Returns(lastEntry.Index);
                m.Setup(p => p.ShouldCreateSnapshot()).Returns(false);
            });

        _ = node.Handle(command);

        _mockPersistence.Verify(p => p.Append(It.IsAny<LogEntry>()), Times.Once());
        _mockPersistence.Verify(p => p.Commit(returnLsn), Times.Once());
    }


    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(10)]
    public void SubmitRequest__КогдаКомандаУспешноРеплицирована__ДолженВыставитьИндексКоммитаВИндексДобавленнойЗаписи(
        int peersCount)
    {
        var term = new Term(1);
        var peers = Enumerable.Range(0, peersCount)
                              .Select((_, _) => CreateDefaultPeer()
                                  .Apply(m =>
                                   {
                                       m.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(),
                                             It.IsAny<CancellationToken>()))
                                        .Returns(AppendEntriesResponse.Ok(term));
                                   }))
                              .ToArray(peersCount);
        var queue = new TaskBackgroundJobQueue();
        var recordLsn = new Lsn(12555);
        var lastEntry = new LogEntryInfo(term, recordLsn - 1);
        using var node = CreateLeaderNode(term, null,
            peers: peers.Select(x => x.Object),
            jobQueue: queue,
            persistenceSetup: m =>
            {
                m.Setup(p => p.Append(It.IsAny<LogEntry>())).Returns(recordLsn);
                m.Setup(p => p.Commit(recordLsn)).Verifiable();
                m.Setup(p => p.TryGetFrom(It.IsAny<Lsn>(), out It.Ref<IReadOnlyList<LogEntry>>.IsAny,
                      out It.Ref<LogEntryInfo>.IsAny))
                 .Callback((Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevEntryInfo) =>
                  {
                      if (index != recordLsn)
                      {
                          throw new InvalidOperationException("Необходимо указать индекс вставленной записи");
                      }

                      entries = new[] {new LogEntry(term, "sample"u8.ToArray())};
                      prevEntryInfo = new LogEntryInfo(term, index - 1);
                  })
                 .Returns(true);
                m.Setup(p => p.GetEntryInfo(It.IsAny<Lsn>())).Returns((Lsn lsn) => new LogEntryInfo(term, lsn));
                m.SetupGet(p => p.LastEntry).Returns(lastEntry);
                m.SetupGet(p => p.CommitIndex).Returns(lastEntry.Index);
                m.Setup(p => p.ShouldCreateSnapshot()).Returns(false);
            });

        _ = node.Handle(123);

        _mockPersistence.Verify(p => p.Commit(recordLsn), Times.Once());
    }

    private static Mock<IPeer> CreateDefaultPeer()
    {
        return new Mock<IPeer>(MockBehavior.Strict).Apply(m =>
        {
            m.SetupGet(x => x.Id).Returns(AnotherNodeId);
        });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void SubmitRequest__КогдаКомандаУспешноРеплицирована__ДолженПрименитьКоманду(int peersCount)
    {
        var term = new Term(1);
        var peers = Enumerable.Range(0, peersCount)
                              .Select((_, _) => CreateDefaultPeer()
                                  .Apply(m =>
                                   {
                                       m.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(),
                                             It.IsAny<CancellationToken>()))
                                        .Returns(AppendEntriesResponse.Ok(term));
                                   }))
                              .ToArray(peersCount);
        var queue = new TaskBackgroundJobQueue();
        var command = 123;
        var commandResponse = 5646353;
        var applicationMock = new Mock<IApplication>().Apply(m =>
        {
            m.Setup(a => a.Apply(command)).Returns(commandResponse).Verifiable();
        });
        var returnLsn = new Lsn(87654);
        var lastEntry = new LogEntryInfo(term, returnLsn - 1);
        using var node = CreateLeaderNode(term, null,
            peers: peers.Select(x => x.Object),
            jobQueue: queue,
            application: applicationMock.Object,
            persistenceSetup: m =>
            {
                m.Setup(p => p.Append(It.IsAny<LogEntry>())).Returns(returnLsn);
                m.Setup(p => p.Commit(returnLsn)).Verifiable();

                m.Setup(p => p.TryGetFrom(It.IsAny<Lsn>(), out It.Ref<IReadOnlyList<LogEntry>>.IsAny,
                      out It.Ref<LogEntryInfo>.IsAny))
                 .Callback((Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevEntryInfo) =>
                  {
                      if (index != returnLsn)
                      {
                          throw new InvalidOperationException("Необходимо указать индекс вставленной записи");
                      }

                      entries = new[] {new LogEntry(term, "sample"u8.ToArray())};
                      prevEntryInfo = new LogEntryInfo(term, index - 1);
                  })
                 .Returns(true);
                m.Setup(p => p.GetEntryInfo(It.IsAny<Lsn>())).Returns((Lsn lsn) => new LogEntryInfo(term, lsn));
                m.SetupGet(p => p.LastEntry).Returns(lastEntry);
                m.SetupGet(p => p.CommitIndex).Returns(lastEntry.Index);
                m.Setup(p => p.ShouldCreateSnapshot()).Returns(false);
            });

        var response = node.Handle(command);

        Assert.Equal(commandResponse, response.Response);
        Assert.True(response.WasLeader);

        applicationMock.Verify(a => a.Apply(It.Is<int>(i => i == command)), Times.Once());
    }

    // TODO: тесты на создание снапшота (ShouldCreateSnasphot())

    [Fact]
    public void
        SubmitRequest__КогдаДругойУзелОтветилБольшимТермомВоВремяРепликации__ДолженВернутьЧтоПересталБытьЛидеромВОтвете()
    {
        var term = new Term(1);
        var peer = CreateDefaultPeer();
        var greaterTerm = term.Increment();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new AppendEntriesResponse(greaterTerm, false));
        var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null, peers: new[] {peer.Object}, jobQueue: queue, persistenceSetup:
            m =>
            {
                var recordLsn = new Lsn(1654673);
                m.Setup(p => p.Append(It.IsAny<LogEntry>())).Returns(recordLsn);
                m.Setup(p => p.Commit(It.IsAny<Lsn>()));
                m.Setup(p => p.GetEntryInfo(It.IsAny<Lsn>())).Returns(LogEntryInfo.Tomb);
                m.Setup(p => p.TryGetFrom(It.IsAny<Lsn>(), out It.Ref<IReadOnlyList<LogEntry>>.IsAny,
                      out It.Ref<LogEntryInfo>.IsAny))
                 .Callback((Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevEntryInfo) =>
                  {
                      entries = new[] {new LogEntry(term, "sample"u8.ToArray())};
                      prevEntryInfo = new LogEntryInfo(term, index - 1);
                  })
                 .Returns(true);
                m.Setup(p => p.UpdateState(greaterTerm, It.IsAny<NodeId?>()));
                m.SetupGet(p => p.LastEntry).Returns(new LogEntryInfo(term, recordLsn));
                m.Setup(p => p.CommitIndex).Returns(recordLsn - 1);
            });
        var response = node.Handle(123);

        Assert.False(response.WasLeader);
        Assert.False(response.HasValue);
    }

    [Fact]
    public void SubmitRequest__КогдаДругойУзелОтветилБольшимТермомВоВремяРепликации__ДолженСтатьПоследователем()
    {
        var term = new Term(1);
        var peer = CreateDefaultPeer();
        var greaterTerm = new Term(term.Value + 545);
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new AppendEntriesResponse(greaterTerm, false));
        var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null, peers: new[] {peer.Object}, jobQueue: queue,
            persistenceSetup: m =>
            {
                var recordLsn = new Lsn(3555);
                m.Setup(p => p.Append(It.IsAny<LogEntry>())).Returns(recordLsn);
                m.Setup(p => p.Commit(It.IsAny<Lsn>()));
                m.Setup(p => p.GetEntryInfo(It.IsAny<Lsn>())).Returns(LogEntryInfo.Tomb);
                m.Setup(p => p.TryGetFrom(It.IsAny<Lsn>(), out It.Ref<IReadOnlyList<LogEntry>>.IsAny,
                      out It.Ref<LogEntryInfo>.IsAny))
                 .Callback((Lsn _, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevEntryInfo) =>
                  {
                      entries = Array.Empty<LogEntry>();
                      prevEntryInfo = LogEntryInfo.Tomb;
                  })
                 .Returns(true);
                m.Setup(p => p.UpdateState(greaterTerm, It.IsAny<NodeId?>()));
                m.SetupGet(p => p.LastEntry).Returns(new LogEntryInfo(term, recordLsn));
                m.Setup(p => p.CommitIndex).Returns(recordLsn - 1);
            });

        _ = node.Handle(123);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void SubmitRequest__КогдаДругойУзелОтветилБольшимТермомВоВремяРепликации__ДолженВыставитьТермВПолученный()
    {
        var term = new Term(1);
        var peer = CreateDefaultPeer();
        var greaterTerm = term.Increment();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new AppendEntriesResponse(greaterTerm, false));
        var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null, peers: new[] {peer.Object}, jobQueue: queue,
            persistenceSetup: m =>
            {
                var recordLsn = new Lsn(1654673);
                m.Setup(p => p.Append(It.IsAny<LogEntry>())).Returns(recordLsn);
                m.Setup(p => p.Commit(It.IsAny<Lsn>()));
                m.Setup(p => p.GetEntryInfo(It.IsAny<Lsn>())).Returns(LogEntryInfo.Tomb);
                m.Setup(p => p.TryGetFrom(It.IsAny<Lsn>(), out It.Ref<IReadOnlyList<LogEntry>>.IsAny,
                      out It.Ref<LogEntryInfo>.IsAny))
                 .Callback((Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevEntryInfo) =>
                  {
                      entries = Array.Empty<LogEntry>();
                      prevEntryInfo = new LogEntryInfo(term, index - 1);
                  })
                 .Returns(true);
                m.Setup(p => p.UpdateState(greaterTerm, It.IsAny<NodeId?>())).Verifiable();
                m.SetupGet(p => p.LastEntry).Returns(new LogEntryInfo(term, recordLsn));
                m.Setup(p => p.CommitIndex).Returns(recordLsn - 1);
            });

        _ = node.Handle(123);

        _mockPersistence.Verify(p => p.UpdateState(greaterTerm, It.IsAny<NodeId?>()), Times.Once());
    }

    [Fact]
    public void SubmitRequest__КогдаДругойУзелОтветилБольшимТермомВоВремяРепликации__ДолженВыставитьVotedForВnull()
    {
        var term = new Term(1);
        var peer = CreateDefaultPeer();
        var greaterTerm = term.Increment();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new AppendEntriesResponse(greaterTerm, false));
        var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null, peers: new[] {peer.Object}, jobQueue: queue,
            persistenceSetup: m =>
            {
                var recordLsn = new Lsn(1654673);
                m.Setup(p => p.Append(It.IsAny<LogEntry>())).Returns(recordLsn);
                m.Setup(p => p.Commit(It.IsAny<Lsn>()));
                m.Setup(p => p.GetEntryInfo(It.IsAny<Lsn>())).Returns(LogEntryInfo.Tomb);
                m.Setup(p => p.TryGetFrom(It.IsAny<Lsn>(), out It.Ref<IReadOnlyList<LogEntry>>.IsAny,
                      out It.Ref<LogEntryInfo>.IsAny))
                 .Callback((Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevEntryInfo) =>
                  {
                      entries = Array.Empty<LogEntry>();
                      prevEntryInfo = new LogEntryInfo(term, index - 1);
                  })
                 .Returns(true);
                m.Setup(p => p.UpdateState(It.IsAny<Term>(), null)).Verifiable();
                m.SetupGet(p => p.LastEntry).Returns(new LogEntryInfo(term, recordLsn));
                m.Setup(p => p.CommitIndex).Returns(recordLsn - 1);
            });

        _ = node.Handle(123);

        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), null), Times.Once());
    }

    private class TaskBackgroundJobQueue : IBackgroundJobQueue
    {
        public void Accept(IBackgroundJob job, CancellationToken token)
        {
            _ = Task.Run(() => job.Run(token), token);
        }
    }
}