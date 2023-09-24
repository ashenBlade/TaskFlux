using System.Net;

namespace TaskFlux.Network.Client;

internal static class EndPointHelpers
{
    public static EndPoint Parse(string address, int defaultPort)
    {
        if (IPEndPoint.TryParse(address, out var ipEndPoint))
        {
            if (ipEndPoint.Port == 0)
            {
                ipEndPoint.Port = defaultPort;
            }

            return ipEndPoint;
        }

        var semicolonIndex = address.IndexOf(':');
        int port;
        string host;
        if (semicolonIndex == -1)
        {
            port = defaultPort;
            host = address;
        }
        else if (int.TryParse(address.AsSpan(semicolonIndex + 1), out port))
        {
            host = address[..semicolonIndex];
        }
        else
        {
            throw new ArgumentException($"Не удалось спарсить адрес порта в адресе {address}");
        }

        if (Uri.CheckHostName(host) != UriHostNameType.Dns)
        {
            throw new ArgumentException(
                $"В переданной строке адреса указана невалидный DNS хост. Полный адрес: {address}. Хост: {host}");
        }

        return new DnsEndPoint(host, port);
    }
}