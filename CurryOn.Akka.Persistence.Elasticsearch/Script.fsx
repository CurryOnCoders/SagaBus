#r @"..\packages\Newtonsoft.Json.10.0.1\lib\net45\Newtonsoft.Json.dll"
#r @"..\packages\Akka.1.2.3\lib\net45\Akka.dll"
#r @"..\packages\Akka.Persistence.1.2.3.43-beta\lib\net45\Akka.Persistence.dll"
#r @"..\packages\Akka.Persistence.FSharp.1.2.3.43-beta\lib\net45\Akka.Persistence.FSharp.dll"
#r @"..\packages\Elasticsearch.Net.5.6.0\lib\net46\Elasticsearch.Net.dll"
#r @"..\packages\FsPickler.3.2.0\lib\net45\FsPickler.dll"
#r @"..\packages\FsPickler.Json.3.2.0\lib\net45\FsPickler.Json.dll"
#r @"..\packages\NEST.5.6.0\lib\net46\Nest.dll"
#r "bin/debug/CurryOn.FSharp.Control.dll"
#r "../CurryOn.Elastic.FSharp/bin/debug/CurryOn.Elastic.FSharp.dll"
#r "bin/debug/CurryOn.Akka.Persistence.Elasticsearch.dll"

open Akka.Persistence.Elasticsearch
open CurryOn.Akka
open CurryOn.Elastic
open FSharp.Control
open System
open Nest

let mappings =
        [{Type = typeof<PersistedEvent>; IndexName = "event_journal"; TypeName = "persisted_event"};
         {Type = typeof<Snapshot>; IndexName = "snapshot_store"; TypeName = "snapshot"};
         {Type = typeof<EventJournalMetadata>; IndexName = "metadata_store"; TypeName = "event_journal_metadata"}]

let client = Elasticsearch.connect {Node = Uri "http://localhost:9200"; DisableDirectStreaming = false; DefaultIndex = Some "event_journal"; RequestTimeout = TimeSpan.FromMinutes 1.0; IndexMappings = mappings}

let settings = 
    let connection = new ConnectionSettings("http://localhost:9200" |> System.Uri)
    connection.MapDefaultTypeIndices(fun mappings -> mappings.Add(typeof<PersistedEvent>, "event_journal")
                                                             .Add(typeof<Snapshot>, "snapshot_store")
                                                             .Add(typeof<EventJournalMetadata>, "metadata_store")
                                                             |> ignore)
let elastic = new ElasticClient(settings)

Query.field<PersistedEvent, string> <@ fun event -> event.PersistenceId @> "EventQueueTransfer-CCAUS-upschargesCache"
|> Query.And (Query.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> (Inclusive 1L) Unbounded)
|> Query.first<PersistedEvent> client None (Sort.descending <@ fun event -> event.SequenceNumber @>)
|> Operation.wait

[Dsl.term<PersistedEvent, string> <@ fun event -> event.PersistenceId @> "EventQueueTransfer-CCAUS-upschargesCache" None;
 Dsl.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> (GreaterThanOrEqual 1L) UnboundedUpper None]
|> Dsl.bool [] [] []
|> Dsl.first<PersistedEvent> client None (Sort.descending <@ fun event -> event.SequenceNumber @>)
|> Operation.wait


Query.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> Unbounded (Inclusive 298L)
|> Query.And (Query.field <@ fun (event: PersistedEvent) -> event.PersistenceId @> "EventQueueSummary-BR01")
|> Query.delete<PersistedEvent> client None None None None
|> Operation.wait


//let query =
//    Query.field <@ fun (event: PersistedEvent) -> event.PersistenceId @> "EventQueueSummary-BR01"
//    |> Query.build
//    |> Search.build None None None None

//elastic.DeleteByQueryAsync(fun d -> 
//    let descriptor = d |> Elastic.applySearchToDeleteByQueryDescriptor query
//    descriptor)

//elastic.DeleteByQueryAsync(fun d -> 
//    let descriptor = d.Query(fun q -> q.QueryString(fun qs -> qs.Query("persistence_id.keyword:EventQueueSummary-BR01") :> IQueryStringQuery))
//    descriptor :> IDeleteByQueryRequest)
