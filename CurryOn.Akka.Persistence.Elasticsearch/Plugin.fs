namespace Akka.Persistence.Elasticsearch

open Akka.Actor
open Akka.Persistence
open Akka.Serialization
open Akka.Streams
open CurryOn.Akka
open CurryOn.Elastic
open FSharp.Control
open System

module internal Settings =
    let mappings =
        [{Type = typeof<PersistedEvent>; IndexName = "event_journal"; TypeName = "persisted_event"};
         {Type = typeof<Snapshot>; IndexName = "snapshot_store"; TypeName = "snapshot"};
         {Type = typeof<EventJournalMetadata>; IndexName = "metadata_store"; TypeName = "event_journal_metadata"}]

    let load (config: Akka.Configuration.Config) =
        { Node = config.GetString("uri", "http://localhost:9200") |> Uri
          DefaultIndex = Some "event_journal"
          DisableDirectStreaming = config.GetBoolean("disable-direct-streaming", false)
          RequestTimeout = config.GetTimeSpan("requestt-timeout", TimeSpan.FromMinutes(1.0) |> Nullable)
          IndexMappings = 
            let configuredMappings = config.GetConfig("index-mappings")
            if configuredMappings |> isNull
            then []
            else configuredMappings.AsEnumerable() |> Seq.map (fun keyValue -> 
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
        { EventId = 0L;
          PersistenceId = persistenceId
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
    let config = system.Settings.Config.GetConfig("akka.persistence.journal.elasticsearch")
    let settings = 
        let elasticSettings = config |> Settings.load
        {elasticSettings with IndexMappings = elasticSettings.IndexMappings @ Settings.mappings}
    let recreateIndices = config.GetBoolean("recreate-indices")
    let connection = settings |> Elasticsearch.connect

    do 
        operation {
            let existsMethod = typeof<IElasticClient>.GetMethod("IndexExists")
            let createMethod = typeof<IElasticClient>.GetMethod("CreateIndex", [||])
            let recreateMethod = typeof<IElasticClient>.GetMethod("RecreateIndex")
            for indexMapping in settings.IndexMappings do
                let exists = existsMethod.MakeGenericMethod(indexMapping.Type)
                let create = createMethod.MakeGenericMethod(indexMapping.Type)
                let recreate = recreateMethod.MakeGenericMethod(indexMapping.Type)
                let! indexExists = exists.Invoke(connection, null) |> unbox<Operation<bool, ElasticsearchEvent>>
                if indexExists |> not
                then let! result = create.Invoke(connection, null) |> unbox<Operation<unit, ElasticsearchEvent>>
                     result
                elif recreateIndices
                then let! result = recreate.Invoke(connection, null) |> unbox<Operation<unit, ElasticsearchEvent>>
                     result
            return! Result.success()
        } |> Operation.returnOrFail

    new (context: IActorContext) = ElasticsearchPlugin(context.System)    
    member __.Connect () = connection
    member __.Config = system.Settings.Config
    member __.Serialization = ElasticsearchSerialization(system)
    member __.Materializer = ActorMaterializer.Create(system)