namespace Akka.Persistence.Elasticsearch

open Akka.Actor
open Akka.Persistence
open Akka.Streams
open CurryOn.Elastic
open System

module internal Settings =
    let load (config: Akka.Configuration.Config) =
        { Node = config.GetString("uri", "http://localhost:9200") |> Uri
          DisableDirectStreaming = config.GetBoolean("disableDirectStreaming", false)
          RequestTimeout = config.GetTimeSpan("requestTimeout", TimeSpan.FromMinutes(1.0) |> Nullable)
          IndexMappings = config.GetConfig("indexMappings").AsEnumerable() |> Seq.map (fun keyValue -> 
            try
                let clrType = Type.GetType(keyValue.Value.GetString())
                match clrType.GetCustomAttributes(true) |> Seq.tryFind (fun attr -> attr.GetType() = typeof<Nest.ElasticsearchTypeAttribute>) with
                | Some attr ->  
                    let typeName = attr |> unbox<Nest.ElasticsearchTypeAttribute> |> fun a -> a.Name
                    Some { Type = clrType; TypeName = typeName; IndexName = keyValue.Key }
                | None -> 
                    None
            with | _ -> None)
            |> Seq.filter (fun opt -> opt.IsSome)
            |> Seq.map (fun opt -> opt.Value)
            |> Seq.toList
        }

type IElasticsearchPlugin =
    inherit IJournalPlugin

type internal ElasticsearchPlugin (system: ActorSystem) =
    let config = system.Settings.Config
    let settings = config.GetConfig("akka.persistence.journal.elasticsearch") |> Settings.load
    let connection = settings |> EventStoreConnection.connect
    new (context: IActorContext) = ElasticsearchPlugin(context.System)    
    member __.Connect () = connection
    member __.Config = config
    member __.Serialization = EventStoreSerialization(system)
    member __.Materializer = ActorMaterializer.Create(system)
    member __.Credentials = UserCredentials(settings.UserName, settings.Password)
