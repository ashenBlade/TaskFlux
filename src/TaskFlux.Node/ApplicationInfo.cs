using TaskFlux.Core;

namespace TaskFlux.Node;

public class ApplicationInfo: IApplicationInfo
{
    public Version Version => Constants.CurrentVersion;
    public override string ToString()
    {
        return $"ApplicationInfo(Version = {Version.ToString()})";
    }
}