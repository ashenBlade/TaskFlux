FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src

COPY ["TaskFlux.Domain/TaskFlux.Domain.csproj", "TaskFlux.Domain/"]
COPY ["TaskFlux.PriorityQueue/TaskFlux.PriorityQueue.csproj", "TaskFlux.PriorityQueue/"]
COPY ["TaskFlux.Core/TaskFlux.Core.csproj", "TaskFlux.Core/"]
COPY ["TaskFlux.Application/TaskFlux.Application.csproj", "TaskFlux.Application/"]

COPY ["TaskFlux.Persistence/TaskFlux.Persistence.csproj", "TaskFlux.Persistence/"]
COPY ["TaskFlux.Consensus/TaskFlux.Consensus.csproj", "TaskFlux.Consensus/"]
COPY ["TaskFlux.Network/TaskFlux.Network.csproj", "TaskFlux.Network/"]

COPY ["TaskFlux.Transport/TaskFlux.Transport.Http/TaskFlux.Transport.Http.csproj", "TaskFlux.Transport/TaskFlux.Transport.Http/"]
COPY ["TaskFlux.Transport/TaskFlux.Transport.Tcp.Common/TaskFlux.Transport.Tcp.Common.csproj", "TaskFlux.Transport/TaskFlux.Transport.Tcp.Common/"]
COPY ["TaskFlux.Transport/TaskFlux.Transport.Tcp.Server/TaskFlux.Transport.Tcp.Server.csproj", "TaskFlux.Transport/TaskFlux.Transport.Tcp.Server/"]
COPY ["TaskFlux.Transport/TaskFlux.Transport.Grpc/TaskFlux.Transport.Grpc.csproj", "TaskFlux.Transport/TaskFlux.Transport.Grpc/"]
COPY ["TaskFlux.Host/TaskFlux.Host.csproj", "TaskFlux.Host/"]
RUN dotnet restore "TaskFlux.Host/TaskFlux.Host.csproj"
COPY . .
WORKDIR "/src/TaskFlux.Host"
RUN dotnet build "TaskFlux.Host.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "TaskFlux.Host.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "TaskFlux.Host.dll"]
