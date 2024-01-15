using TaskFlux.Utils.CheckSum;
using Xunit;

namespace TaskFlux.Utils.Tests;

[Trait("Category", "Infrastructure")]
public class Crc32CheckSumTests
{
    [Fact]
    public void Compute__МодельнаяПроверка__ДолженПосчитатьПравильно()
    {
        uint expected = 0x376E6E7;
        var data = "123456789"u8.ToArray();
        var actual = Crc32CheckSum.Compute(data);
        Assert.Equal(expected, actual);
    }

    public static IEnumerable<object[]> РазныеЗначения => new[]
    {
        new object[] {"hello, world"u8.ToArray(), 0xBD0822B5}, new object[]
        {
            "w[goiaqw[bgnoqaiwrej[goqweij[fmgwqepoij[gfwoniebgj[n'dlckag[hjrogna'wrigj6W4W462&%&q2SVDSGEARH"u8
               .ToArray(),
            0xB1C079A3
        },
        new object[] {new byte[] {0x01, 0x22, 0xF2, 0x34, 0x8A, 0xA0}, 0x1EDB283B},
        new object[] {new byte[] {0x84, 0x23, 0xBA, 0x3B, 0x32, 0x90, 0xCC, 0x27, 0x33, 0x50, 0xD2}, 0xE70CBF17}
    };

    [Theory]
    [MemberData(nameof(РазныеЗначения))]
    public void Compute__РазныеЗначения__ДолженПосчитатьПравильно(byte[] data, uint expected)
    {
        var actual = Crc32CheckSum.Compute(data);
        Assert.Equal(expected, actual);
    }
}