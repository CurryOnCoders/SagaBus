namespace Akka.Persistence.Elasticsearch

open Akka.Actor
open Akka.Persistence
open Akka.Serialization
open Akka.Streams
open CurryOn.Elastic
open FSharp.Control
open System

module internal Settings =
    let load (config: Akka.Configuration.Config) =
        { Node = config.GetString("uri", "http://localhost:9200") |> Uri
          DisableDirectStreaming = config.GetBoolean("disable-direct-streaming", false)
          RequestTimeout = config.GetTimeSpan("requestt-timeout", TimeSpan.FromMinutes(1.0) |> Nullable)
          IndexMappings = config.GetConfig("index-mappings").AsEnumerable() |> Seq.map (fun keyValue -> 
            try
                let clrType = Type.GetType(keyValue.Value.GetString())
                match clrType.GetCustomAttributes(typeof<Nest.ElasticsearchTypeAttribute>, true) with
                | [||] -> None
                | array ->  
                    let typeName = array |> Seq.head |> unbox<Nest.ElasticsearchTypeAttribute> |> fun a -> a.Name
                    Some { Type = clrType; TypeName = typeName; IndexName = keyValue.Key }                
            with | _ -> None)
            |> Seq.filter (fun opt -> opt.IsSome)
            |> Seq.map (fun opt -> opt.Value)
            |> Seq.toList
        }

type ElasticsearchSerialization (serialization: Serialization) =
    new (actorSystem: ActorSystem) = ElasticsearchSerialization(actorSystem.Serialization)
    member __.Serialize (persistenceId, sender, sequenceNr, manifest, writerGuid, payload) =
        { PersistenceId = persistenceId
          SequenceNumber = sequenceNr
          EventType = manifest
          Sender = sender
          Event = payload |> box |> Serialization.toJson
          WriterId = writerGuid
          Tags = [||]
        }
    member __.Deserialize<'a> (persistedEvent: PersistedEvent) =
        persistedEvent.Event |> Serialization.parseJson<obj> |> unbox<'a>


type IElasticsearchPlugin =
    inherit IJournalPlugin

type ElasticsearchPlugin (system: ActorSystem) =
    let config = system.Settings.Config
    let settings = config.GetConfig("akka.persistence.journal.elasticsearch") |> Settings.load
    let connection = settings |> Elasticsearch.connect

    do 
        operation {
            let existsMethod = typeof<IElasticClient>.GetMethod("IndexExists")
            let createMethod = typeof<IElasticClient>.GetMethod("CreateIndex", [||])
            for indexMapping in settings.IndexMappings do
                let exists = existsMethod.MakeGenericMethod(indexMapping.Type)
                let create = createMethod.MakeGenericMethod(indexMapping.Type)
                let! indexExists = exists.Invoke(connection, null) |> unbox<Operation<bool, ElasticsearchEvent>>
                if indexExists |> not
                then let! result = create.Invoke(connection, null) |> unbox<Operation<unit, ElasticsearchEvent>>
                     result
            return! Result.success()
        } |> Operation.returnOrFail

    new (context: IActorContext) = ElasticsearchPlugin(context.System)    
    member __.Connect () = connection
    member __.Config = config
    member __.Serialization = ElasticsearchSerialization(system)
    member __.Materializer = ActorMaterializer.Create(system)