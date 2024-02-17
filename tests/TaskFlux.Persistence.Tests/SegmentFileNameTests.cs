using TaskFlux.Consensus;
using TaskFlux.Persistence.Log;
using Xunit;

namespace TaskFlux.Persistence.Tests;

[Trait("Category", "Persistence")]
public class SegmentFileNameTests
{
    public static IEnumerable<object[]> LsnFileNames => new object[][]
    {
        [0, "0000000000000000000.log"], [1, "0000000000000000001.log"], [7654, "0000000000000007654.log"],
        [1000000000000000000, "1000000000000000000.log"], [long.MaxValue, "9223372036854775807.log"],
        [3333333333332, "0000003333333333332.log"],
    };

    [Theory]
    [MemberData(nameof(LsnFileNames))]
    public void GetFileName__ДолженВернутьКорректноеИмяФайла(long lsn, string expected)
    {
        var name = new SegmentFileName(lsn);

        var actual = name.GetFileName();

        Assert.Equal(expected, actual);
    }

    public static IEnumerable<object[]> ValidFileNames => LsnFileNames.Select(arr => new[] {arr[1]});

    [Theory]
    [MemberData(nameof(ValidFileNames))]
    public void TryParse__КогдаПереданКорректноеНазваниеФайла__ДолженВернутьTrue(string filename)
    {
        var actual = SegmentFileName.TryParse(filename, out _);

        Assert.True(actual);
    }

    [Theory]
    [MemberData(nameof(LsnFileNames))]
    public void TryParse__КогдаПереданКорректноеНазваниеФайла__ДолженВернутьКорректныйИндексНачала(
        long expected,
        string filename)
    {
        _ = SegmentFileName.TryParse(filename, out var name);

        Assert.Equal(new Lsn(expected), name.StartLsn);
    }

    [Theory]
    [InlineData("")]
    [InlineData("ddd")]
    [InlineData(".log")]
    [InlineData("0.log")]
    [InlineData("34679iejyw")]
    [InlineData("xxxxxxxxx")]
    [InlineData("metadata")]
    public void TryParse__КогдаПередаетсяНеВалидноеИмяФайла__ДолженВернутьFalse(string filename)
    {
        var success = SegmentFileName.TryParse(filename, out _);

        Assert.False(success);
    }
}