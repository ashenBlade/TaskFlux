FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["Raft.Server/Raft.Server.csproj", "Raft.Server/"]
COPY ["Raft.Timers/Raft.Timers.csproj", "Raft.Timers/"]
COPY ["Raft.Core/Raft.Core.csproj", "Raft.Core/"]
COPY ["Raft.Peer/Raft.Peer.csproj", "Raft.Peer/"]
COPY ["Raft.JobQueue/Raft.JobQueue.csproj", "Raft.JobQueue/"]
COPY ["Raft.Log/Raft.Log.csproj", "Raft.Log/"]
RUN dotnet restore "Raft.Server/Raft.Server.csproj"
COPY . .
WORKDIR "/src/Raft.Server"
RUN dotnet build "Raft.Server.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Raft.Server.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Raft.Server.dll"]
