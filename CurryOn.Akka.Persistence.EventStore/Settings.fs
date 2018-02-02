namespace CurryOn.Akka

open Akka.Configuration
open CurryOn.Common
open EventStore.ClientAPI
open System
open System.Net

type Settings =
    {
        ServerName: string;
        TcpPort: int;
        HttpPort: int;
        GossipPort: int;
        UseCluster: bool;
        ClusterDns: string;
        UserName: string;
        Password: string;
        MetadataStream: string;
        VerboseLogging: bool;
        KeepReconnecting: bool;
        MaxReconnectionAttempts: int;
        MaxOperationAttempts: int;
        MaxConcurrentOperations: int;
        KeepRetrying: bool;
        MaxRetryAttempts: int;
        MaxQueueDepth: int;
        HeartbeatInterval: TimeSpan;
        HeartbeatTimeout: TimeSpan;
        OperationTimeout: TimeSpan;
    }
    static member Load (config: Config) =
        {
            ServerName = config.GetString("server-name")
            TcpPort = config.GetInt("tcp-port", 1113)
            HttpPort = config.GetInt("http-port", 2113)
            GossipPort = config.GetInt("gossip-port", 30777) 
            UseCluster = config.GetBoolean("use-cluster", false)
            ClusterDns = config.GetString("cluster-dns", "localhost")
            UserName = config.GetString("user-name", "admin")
            Password = config.GetString("user-password", "changeit")
            MetadataStream = config.GetString("metadata-stream", "Akka.Metadata")
            VerboseLogging = config.GetBoolean("verbose-logging", false)
            KeepReconnecting = config.GetBoolean("keep-reconnecting", false)
            MaxReconnectionAttempts = config.GetInt("max-reconnect-attempts", 10)
            KeepRetrying = config.GetBoolean("keep-retrying", false)
            MaxRetryAttempts = config.GetInt("max-retry-attempts", 10)
            MaxOperationAttempts = config.GetInt("max-operation-attempts", 11)
            MaxConcurrentOperations = config.GetInt("max-concurrent-operations", 255)
            MaxQueueDepth = config.GetInt("max-queue-depth", 5000)
            HeartbeatInterval = config.GetTimeSpan("heartbeat-interval", TimeSpan.FromMilliseconds(750.0) |> Nullable)
            HeartbeatTimeout = config.GetTimeSpan("heartbeat-timeout", TimeSpan.FromMilliseconds(1500.0) |> Nullable)
            OperationTimeout = config.GetTimeSpan("operation-timeout", TimeSpan.FromSeconds(7.0) |> Nullable)
        }
    member settings.ConnectionString = 
        if settings.UseCluster
        then sprintf "ConnectTo=discover://%s:%s@%s:%d;" settings.UserName settings.Password settings.ServerName settings.GossipPort
        else sprintf "ConnectTo=tcp://%s:%s@%s:%d;" settings.UserName settings.Password settings.ServerName settings.TcpPort

 module EventStoreConnection =
    let create (settings: Settings) =
        let connectionSettings = 
            let applySettingIf condition (apply: ConnectionSettingsBuilder -> ConnectionSettingsBuilder) inputSettings = 
                if condition then inputSettings |> apply else inputSettings 
            ConnectionSettings.Create()
                               .LimitAttemptsForOperationTo(settings.MaxOperationAttempts)
                               .LimitConcurrentOperationsTo(settings.MaxConcurrentOperations)
                               .LimitOperationsQueueTo(settings.MaxQueueDepth)
                               .LimitReconnectionsTo(settings.MaxReconnectionAttempts)
                               .LimitRetriesForOperationTo(settings.MaxRetryAttempts)
                               .SetDefaultUserCredentials(SystemData.UserCredentials(settings.UserName, settings.Password))
                               .SetHeartbeatInterval(settings.HeartbeatInterval)
                               .SetHeartbeatTimeout(settings.HeartbeatTimeout)
                               .SetOperationTimeoutTo(settings.OperationTimeout)
            |> applySettingIf settings.VerboseLogging (fun s -> s.EnableVerboseLogging())
            |> applySettingIf settings.KeepReconnecting (fun s -> s.KeepReconnecting())
            |> applySettingIf settings.KeepRetrying (fun s -> s.KeepRetrying())
            |> applySettingIf settings.UseCluster (fun s -> s.SetClusterDns(settings.ClusterDns))
            |> applySettingIf settings.UseCluster (fun s-> s.SetClusterGossipPort(settings.GossipPort))

        EventStoreConnection.Create(settings.ConnectionString, connectionSettings)

    let connect settings =
        task {
            let connection = create settings
            do! connection.ConnectAsync() |> Task.ofUnit
            return connection
        }
