namespace Akka.Persistence.Elasticsearch

open Akka.Actor
open Akka.Configuration
open Akka.Persistence
open Akka.Persistence.Snapshot
open CurryOn.Akka
open CurryOn.Common
open CurryOn.Elastic
open FSharp.Control
open System
open System.Threading.Tasks

type ElasticsearchSnapshotStore (config: Config) =
    inherit SnapshotStore()
    let context = SnapshotStore.Context
    let plugin = ElasticsearchPlugin(context)
    let readBatchSize = lazy(config.GetInt("read-batch-size"))
    let client = plugin.Connect()    

    let getSnapshotQuery persistenceId (criteria: SnapshotSelectionCriteria) =
        let inline getTimestampBound (date: Nullable<DateTime>) =
            if date.HasValue
            then Inclusive date.Value
            else Unbounded

        let inline getSequenceNrBound (min: int64) =
            if min > 0L
            then Inclusive min
            else Unbounded

        Query.field<Snapshot,string> <@ fun snapshot -> snapshot.PersistenceId @> persistenceId
        |> Query.And (Query.range<Snapshot,int64> <@ fun snapshot -> snapshot.SequenceNumber @> (getSequenceNrBound criteria.MinSequenceNr) (Inclusive criteria.MaxSequenceNr))
        |> Query.And (Query.dateRange<Snapshot,DateTime> <@ fun snapshot -> snapshot.Timestamp @> (getTimestampBound criteria.MinTimestamp) (Inclusive criteria.MaxTimeStamp))

    override __.SaveAsync (metadata, snapshot) =
        task {
            let persistedSnapshot = 
                { PersistenceId = metadata.PersistenceId
                  SnapshotType = snapshot.GetType().FullName
                  SequenceNumber = metadata.SequenceNr
                  Timestamp = metadata.Timestamp.ToUniversalTime()
                  State = snapshot |> Serialization.toJson
                }
            let! result = client.Index({ Id = None; Document = persistedSnapshot }) |> Operation.waitTask
            return! result |> SearchResult.toTask
        } :> Task

    override __.LoadAsync (persistenceId, criteria) =
        task {            
            let! firstSnapshot =
                getSnapshotQuery persistenceId criteria
                |> Query.first<Snapshot> client None (Sort.descending <@ fun snapshot -> snapshot.SequenceNumber @>)
                |> SearchOperation.toTask
            match firstSnapshot with
            | Some snapshot -> return SelectedSnapshot(SnapshotMetadata(snapshot.PersistenceId, snapshot.SequenceNumber, snapshot.Timestamp), snapshot.State |> Serialization.parseJson<obj>)
            | None -> return null
        }
    
    override __.DeleteAsync (metadata) =
        task {
            let! result = 
                if metadata.Timestamp > DateTime.MinValue
                then SnapshotSelectionCriteria(metadata.SequenceNr, metadata.Timestamp) |> getSnapshotQuery metadata.PersistenceId 
                     |> Query.delete client None None None None
                     |> Operation.waitTask
                else Query.field<Snapshot,string> <@ fun snapshot -> snapshot.PersistenceId @> metadata.PersistenceId
                     |> Query.And (Query.field<Snapshot,int64> <@ fun snapshot -> snapshot.SequenceNumber @> metadata.SequenceNr)
                     |> Query.delete client None None None None
                     |> Operation.waitTask

            return! result |> SearchResult.toTask
        } :> Task

    override __.DeleteAsync (persistenceId, criteria) =
        task {
            return!
                getSnapshotQuery persistenceId criteria
                |> Query.delete client None None None None
                |> SearchOperation.toTask
        } :> Task