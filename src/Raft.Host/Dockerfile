FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY . .
RUN dotnet restore "Raft.Host/Raft.Host.csproj"
WORKDIR "/src/Raft.Host"
RUN dotnet build "Raft.Host.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Raft.Host.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Raft.Host.dll"]
