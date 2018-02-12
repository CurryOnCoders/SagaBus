namespace CurryOn.Akka

open Akka.Configuration
open FSharp.Control
open Kafunk
open System

type Settings =
    {
        ServerUris: string list
        ClientId: string
        KafkaServerVersion: string
        UseDynamicApiVersion: bool        
        ConnectionTimeout: TimeSpan
        ConnectionRetries: int
        ConnectionRetryDelay: TimeSpan
        RequestTimeout: TimeSpan
        RequestRetries: int
        RequestRetryDelay: TimeSpan
        UseNaglesAlgorithm: bool
        ReceiveBufferSize: int
        SendBufferSize: int
    }
    static member Load (config: Config) =
        {
            KafkaServerVersion = config.GetString("kafka-server-version", "0.10.1")
            ClientId = config.GetString("client-id")
            UseDynamicApiVersion = config.GetBoolean("use-dynamic-api-version", false)
            ServerUris = config.GetStringList("server-uris") |> Seq.toList
            ConnectionTimeout = config.GetTimeSpan("connection-timeout", Nullable <| TimeSpan.FromSeconds 10.0)
            ConnectionRetries = config.GetInt("connection-retries", 3)
            ConnectionRetryDelay = config.GetTimeSpan("connection-retry-delay", Nullable <| TimeSpan.FromSeconds 2.0)
            RequestTimeout = config.GetTimeSpan("request-timeout", Nullable <| TimeSpan.FromSeconds 30.0)
            RequestRetries = config.GetInt("request-retries", 10)
            RequestRetryDelay = config.GetTimeSpan("request-retry-delay", Nullable <| TimeSpan.FromSeconds 1.0)
            UseNaglesAlgorithm = config.GetBoolean("use-nagles-algorithm", false)
            ReceiveBufferSize = config.GetInt("receive-buffer-size", 65535)
            SendBufferSize = config.GetInt("send-buffer-size", 65535)
        }

type KafkaConnectionEvents =
| ConnectedSuccessfully
| ConnectionFailed of exn

 module KafkaConnection =
    let create (settings: Settings) =
        operation {
            let kafkaConfig = KafkaConfig.create (settings.ServerUris |> List.map Uri, 
                                                  settings.ClientId, 
                                                  ChanConfig.create (settings.UseNaglesAlgorithm, 
                                                                     settings.ReceiveBufferSize, 
                                                                     settings.SendBufferSize, 
                                                                     settings.ConnectionTimeout, 
                                                                     RetryPolicy.constantBounded settings.ConnectionRetryDelay settings.ConnectionRetries, 
                                                                     settings.RequestTimeout, 
                                                                     RetryPolicy.constantBounded settings.RequestRetryDelay settings.RequestRetries, 
                                                                     BufferPool.GC), 
                                                  RetryPolicy.constantBounded settings.ConnectionRetryDelay settings.ConnectionRetries, 
                                                  RetryPolicy.constantBounded settings.RequestRetryDelay settings.RequestRetries, 
                                                  Version.Parse settings.KafkaServerVersion, 
                                                  settings.UseDynamicApiVersion)
                                                   
            let! connection = Kafka.connAsync kafkaConfig
            return! Result.successWithEvents connection [ConnectedSuccessfully]
        }
