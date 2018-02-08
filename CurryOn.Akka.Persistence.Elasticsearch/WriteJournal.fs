namespace Akka.Persistence.Elasticsearch

open Akka
open Akka.Actor
open Akka.Configuration
open Akka.Event
open Akka.FSharp
open Akka.Persistence
open Akka.Persistence.Journal
open Akka.Streams
open Akka.Streams.Dsl
open CurryOn.Akka
open CurryOn.Common
open CurryOn.Elastic
open FSharp.Control
open System
open System.Collections.Concurrent
open System.Collections.Immutable
open System.Threading
open System.Threading.Tasks

module EventJournal = 
    let get (config: Config) (context: IActorContext) =
        let plugin = ElasticsearchPlugin(context)
        let writeBatchSize = config.GetInt("write-batch-size", 512)
        let readBatchSize = config.GetInt("read-batch-size", 1024)
        let client = plugin.Connect () 

        let toPersistedEvent (event: JournaledEvent) =
            { PersistenceId = event.PersistenceId
              SequenceNumber = event.SequenceNumber
              EventType = event.Manifest
              Sender = event.Sender
              WriterId = event.WriterId
              Event = event.Event |> Serialization.toJson
              Tags = event.Tags
            }

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

        {new IEventJournal with
            member __.GetCurrentPersistenceIds () =
                operation {
                    let! result = client |> Search.distinct<PersistedEvent,string> <@ fun event -> event.PersistenceId @> None
                    let persistenceIds = result.Results.Hits |> List.fold (fun acc hit -> acc |> Set.add hit.Document.PersistenceId) Set.empty<string> 
                    return! Result.success persistenceIds
                }
            member __.PersistEvents events =
                operation {                    
                    match events with
                    | [] -> 
                        return! Result.success ()
                    | [event] ->
                        let persistedEvent = event |> toPersistedEvent                            
                        let! result = client.Index({ Id = None; Document = persistedEvent})                            
                        return! Result.successWithEvents () [PersistedSuccessfully]
                    | events -> 
                        let! result = client.BulkIndex(events |> Seq.map toPersistedEvent)                            
                        return! Result.successWithEvents () [PersistedSuccessfully]
                }
            member __.DeleteEvents persistenceId upperLimit =
                operation {
                    let! result =
                        Query.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> Unbounded (Inclusive upperLimit)
                        |> Query.And (Query.field <@ fun (event: PersistedEvent) -> event.PersistenceId @> persistenceId)
                        |> Query.delete<PersistedEvent> client None None None None

                    return! Result.successWithEvents () [DeletedSuccessfully]
                }
            member __.GetMaxSequenceNumber persistenceId from =
                operation {
                    let! highestSequence = 
                        Query.field<PersistedEvent, string> <@ fun event -> event.PersistenceId @> persistenceId
                        |> Query.And (Query.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> (Inclusive from) Unbounded)
                        |> Query.first<PersistedEvent> client None (Sort.descending <@ fun event -> event.SequenceNumber @>)
                
                    return! match highestSequence with
                            | Some event -> event.SequenceNumber |> Some
                            | None -> None
                            |> Operation.success
                }
            member __.GetEvents persistenceId first last max =
                operation {
                    let! result =
                        Query.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> (Inclusive first) (Inclusive last)
                        |> Query.And (Query.field <@ fun (event: PersistedEvent) -> event.PersistenceId @> persistenceId)
                        |> Query.execute<PersistedEvent> client None (Some <| Sort.ascending <@ fun event -> event.SequenceNumber @>) None (max |> int |> Some)

                    return!
                        result.Results.Hits 
                        |> Seq.map (fun hit -> hit.Document :> IPersistentRepresentation)
                        |> Result.success
                }
            member __.GetTaggedEvents tag lowOffset highOffset =
                operation {
                    let! result = PersistenceQuery.getCurrentEventsByTag client tag lowOffset highOffset
                    return! result.Results.Hits 
                            |> Seq.map (fun hit -> { Tag = tag; Event = hit.Document; Id = hit.Id.ToInt() })    
                            |> Result.success
                }
            member __.SaveSnapshot snapshot =
                operation {
                    let persistedSnapshot = 
                        { PersistenceId = snapshot.PersistenceId
                          SnapshotType = snapshot.Manifest
                          SequenceNumber = snapshot.SequenceNumber
                          Timestamp = snapshot.Timestamp.ToUniversalTime()
                          State = snapshot.Snapshot |> Serialization.toJson
                        }
            
                    let! result = client.Index({ Id = None; Document = persistedSnapshot })

                    return! Result.successWithEvents () [PersistedSuccessfully]
                }
            member __.GetSnapshot persistenceId criteria =
                operation {
                    let! firstSnapshot =
                        getSnapshotQuery persistenceId criteria
                        |> Query.first<Snapshot> client None (Sort.descending <@ fun snapshot -> snapshot.SequenceNumber @>)

                    return! match firstSnapshot with
                            | Some snapshot -> Some {PersistenceId = snapshot.PersistenceId; 
                                                     Manifest = snapshot.SnapshotType; 
                                                     SequenceNumber = snapshot.SequenceNumber; 
                                                     Timestamp = snapshot.Timestamp; 
                                                     Snapshot = snapshot.State |> Serialization.parseJson<obj>}
                            | None -> None
                            |> Result.success
                }
            member __.DeleteSnapshots persistenceId criteria =
                operation {
                    let! result = criteria |> getSnapshotQuery persistenceId |> Query.delete<Snapshot> client None None None None
                    return! Result.successWithEvents () [DeletedSuccessfully]
                }
            member __.DeleteAllSnapshots persistenceId sequenceNumber =
                operation {
                    let! result = 
                         Query.field<Snapshot,string> <@ fun snapshot -> snapshot.PersistenceId @> persistenceId
                         |> Query.And (Query.field<Snapshot,int64> <@ fun snapshot -> snapshot.SequenceNumber @> sequenceNumber)
                         |> Query.delete<Snapshot> client None None None None

                    return! Result.successWithEvents () [DeletedSuccessfully]
                }
        }


type ElasticsearchProvider() =
    interface IEventJournalProvider with
        member __.GetEventJournal config context = EventJournal.get config context

type ElasticsearchJournal (config: Config) =
    inherit IndexedEventJournal<ElasticsearchProvider>(config)
    static member Identifier = "akka.persistence.journal.elasticsearch"

type ElasticsearchSnapshotStore (config: Config) =
    inherit SnapshotStoreBase<ElasticsearchProvider>(config)
    static member Identifier = "akka.persistence.snapshot-store.elasticsearch"