#r @"..\packages\Elasticsearch.Net.5.6.0\lib\net46\Elasticsearch.Net.dll"
#r @"..\packages\NEST.5.6.0\lib\net46\Nest.dll"
#r @"..\CurryOn.Akka.Persistence.Elasticsearch\bin\Debug\CurryOn.Akka.Persistence.Elasticsearch.dll"

open CurryOn.Akka
open Nest

let settings = 
    let connection = new ConnectionSettings("http://localhost:9200" |> System.Uri)
    connection.MapDefaultTypeIndices(fun mappings -> mappings.Add(typeof<PersistedEvent>, "event_journal")
                                                             .Add(typeof<Snapshot>, "snapshot_store")
                                                             .Add(typeof<EventJournalMetadata>, "metadata_store")
                                                             |> ignore)
let client = new ElasticClient(settings)

client.DeleteByQuery<PersistedEvent>(fun q -> q.MatchAll() :> IDeleteByQueryRequest)
client.DeleteByQuery<Snapshot>(fun q -> q.MatchAll() :> IDeleteByQueryRequest)
client.DeleteByQuery<EventJournalMetadata>(fun q -> q.MatchAll() :> IDeleteByQueryRequest)