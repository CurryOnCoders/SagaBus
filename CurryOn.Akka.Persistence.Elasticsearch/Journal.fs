namespace Akka.Persistence.Elasticsearch

open Akka.Actor
open Akka.Configuration
open Akka.Persistence
open Akka.Persistence.Journal
open Akka.Streams
open Akka.Streams.Dsl
open CurryOn.Common
open CurryOn.Elastic
open FSharp.Control
open System
open System.Collections.Immutable
open System.Threading
open System.Threading.Tasks

module internal EventJournal =
    let searchForType = memoize <| Types.findType        

    let getEventType (persistedEvent: PersistedEvent) =
        searchForType persistedEvent.EventType |> Operation.returnOrFail

    let deserialize (serialization: ElasticsearchSerialization) (eventType: Type) (event: PersistedEvent) =
        let deserializer = serialization.GetType().GetMethod("Deserialize").MakeGenericMethod(eventType)
        deserializer.Invoke(serialization, [|event|])

type ElasticsearchJournal (config: Config) = 
    inherit AsyncWriteJournal()
    static let semaphore = new SemaphoreSlim(1, 1)
    static let journalId = Guid.NewGuid()
    let context = AsyncWriteJournal.Context
    let plugin = ElasticsearchPlugin(context)
    let writeBatchSize = lazy(config.GetInt("write-batch-size"))
    let readBatchSize = lazy(config.GetInt("read-batch-size"))
    let client = plugin.Connect () 

    let maximumEventId =
        operation {
            let! eventMetadata = 
                Dsl.matchAll<EventJournalMetadata> None
                |> Dsl.first<EventJournalMetadata> client None (Sort.descending <@ fun metadata -> metadata.CommitDate @>)                    
            return! Result.success <|
                match eventMetadata with
                | Some metadata -> metadata.MaximumEventId
                | None -> 0L
        } |> Operation.returnOrFail |> ref

    let getEventIds numberOfIds =
        operation {          
            do! semaphore.WaitAsync()
            try 
                let eventIds = [for _ in {1..numberOfIds} do yield Interlocked.Increment(maximumEventId)]
                let! metaResult = client.Index({ Id = None; Document = {MaximumEventId = !maximumEventId; CommitDate = DateTime.UtcNow}})
                return! Result.success eventIds
            finally 
                semaphore.Release() |> ignore
        }

    override this.WriteMessagesAsync messages =
        task {
            let indexOperations = 
                messages 
                |> Seq.map (fun message ->
                    operation {
                        let persistentMessages =  message.Payload |> unbox<IImmutableList<IPersistentRepresentation>> 
                        let events = persistentMessages |> Seq.map (fun persistentMessage ->
                            let eventType = persistentMessage.Payload |> getTypeName
                            let tags = 
                                match persistentMessage |> box with
                                | :? Tagged as tagged -> tagged.Tags |> Seq.toArray
                                | _ -> [||] 
                            { EventId = 0L;
                              PersistenceId = persistentMessage.PersistenceId 
                              EventType = persistentMessage.Payload |> getFullTypeName
                              Sender = persistentMessage.Sender
                              SequenceNumber = persistentMessage.SequenceNr
                              Event = persistentMessage.Payload |> Serialization.toJson
                              WriterId = persistentMessage.WriterGuid
                              Tags = tags}) |> Seq.toList

                        let! eventIds = getEventIds events.Length

                        match events |> List.mapi (fun index event -> {event with EventId = eventIds.[index]}) with
                        | [] -> 
                            return! Result.success List<DocumentId>.Empty
                        | [event] ->
                            let! result = client.Index({ Id = Some <| IntegerId event.EventId; Document = event})
                            return! Result.success [result.Id]
                        | events -> 
                            let! result = client.BulkIndex(events)
                            return! result.Results |> List.map (fun r -> r.Id) |> Result.success
                    })          
                |> Operation.Parallel
        
            let! results = indexOperations |> Async.StartAsTask
            let errors = results |> Array.fold (fun acc cur ->
                match cur with
                | Success _ -> acc
                | Failure events -> 
                    let exceptions = 
                        events 
                        |> List.map (fun event -> event.ToException()) 
                        |> List.filter (fun opt -> opt.IsSome) 
                        |> List.map (fun opt -> opt.Value)
                    acc @ exceptions) List<exn>.Empty
            return match errors with
                   | [] -> null
                   | _ -> ImmutableList.CreateRange(errors) :> IImmutableList<exn>
        }

    override this.DeleteMessagesToAsync (persistenceId, sequenceNumber) =
        task {
            return! Query.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> Unbounded (Inclusive sequenceNumber)
                    |> Query.And (Query.field <@ fun (event: PersistedEvent) -> event.PersistenceId @> persistenceId)
                    |> Query.delete client None None None None
                    |> SearchOperation.toTask
        } :> Task

    override this.ReadHighestSequenceNrAsync (persistenceId, from) =
        task {
            let! highestSequence = 
                Query.field<PersistedEvent, string> <@ fun event -> event.PersistenceId @> persistenceId
                |> Query.And (Query.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> (Inclusive from) Unbounded)
                |> Query.first<PersistedEvent> client None (Sort.descending <@ fun event -> event.SequenceNumber @>)
                |> Operation.waitTask
            return match highestSequence with
                   | Success success -> 
                        match success.Result with
                        | Some result -> result.SequenceNumber
                        | None -> 0L
                   | _ -> 0L
        }
   
    override this.ReplayMessagesAsync (context, persistenceId, first, last, max, recoveryCallback) =
        task {
            let! result =
                Query.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> (Inclusive first) (Inclusive last)
                |> Query.And (Query.field <@ fun (event: PersistedEvent) -> event.PersistenceId @> persistenceId)
                |> Query.execute<PersistedEvent> client None None None (max |> int |> Some)
                |> SearchOperation.toTask
            for hit in result.Results.Hits do
                hit.Document |> recoveryCallback.Invoke
        } :> Task
    