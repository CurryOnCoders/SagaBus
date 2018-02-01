namespace Akka.Persistence.EventStore

open Akka.Actor
open Akka.Configuration
open Akka.Persistence
open Akka.Persistence.EventStore
open Akka.Persistence.Journal
open Akka.Streams
open Akka.Streams.Dsl
open CurryOn.Akka
open CurryOn.Common
open FSharp.Control
open EventStore.ClientAPI
open Microsoft.VisualStudio.Threading
open System
open System.Collections.Immutable
open System.Threading
open System.Threading.Tasks

module internal EventJournal =
    let getEventType (resolvedEvent: ResolvedEvent) =
        let eventType = resolvedEvent.Event.EventType
        searchForType eventType |> Operation.returnOrFail

    let deserialize (serialization: EventStoreSerialization) (eventType: Type) (event: RecordedEvent) =
        let deserializer = serialization.GetType().GetMethod("Deserialize").MakeGenericMethod(eventType)
        deserializer.Invoke(serialization, [|event|])

    let deserializeEvent (serialization: EventStoreSerialization) (resolvedEvent: ResolvedEvent) =
        let eventType = resolvedEvent |> getEventType
        resolvedEvent.Event |> deserialize serialization eventType

type EventStoreJournal (config: Config, context: IActorContext) = 
    static do EventJournal.register<EventStoreJournal> (fun config context -> EventStoreJournal(config, context) :> IEventJournal)
    let plugin = EventStorePlugin(context)
    let connect () = plugin.Connect () 
    let readBatchSize = config.GetInt("read-batch-size", 4095)

    interface IEventJournal with
        member __.GetCurrentPersistenceIds () =
            operation {
                let rec readSlice startPosition ids =
                    task {
                        let! eventStore = connect()
                        let! eventSlice = eventStore.ReadStreamEventsForwardAsync("$streams", startPosition, readBatchSize, false, userCredentials = plugin.Credentials)
                        let newIds = eventSlice.Events |> Seq.map (fun event -> event.OriginalStreamId) |> Seq.fold (fun acc cur -> acc |> Set.add cur) ids
                        if eventSlice.IsEndOfStream |> not
                        then return! newIds |> readSlice eventSlice.NextEventNumber
                        else return newIds
                    }
                let! persistenceIds = Set.empty<string> |> readSlice 0L
                return! Result.success persistenceIds
            }
        member __.PersistEvents events =
            operation {
                match events with
                | [] -> 
                    return! Result.success ()
                | [event] ->
                    let! result = client.Index({ Id = None; Document = event})                            
                    return! Result.successWithEvents () [PersistedSuccessfully]
                | events -> 
                    let! result = client.BulkIndex(events)                            
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

    override this.WriteMessagesAsync messages =
        task {
            let! eventStore = connect()
            let tasks = messages 
                        |> Seq.map (fun message ->
                            let persistentMessages =  message.Payload |> unbox<IImmutableList<IPersistentRepresentation>> 
                            let events = persistentMessages |> Seq.map (fun persistentMessage ->
                                let eventType = persistentMessage.Payload |> getTypeName
                                let tags = 
                                    match persistentMessage |> box with
                                    | :? Tagged as tagged -> tagged.Tags |> Seq.toArray
                                    | _ -> [||] 
                                let eventMetadata = {EventType = persistentMessage.Payload |> getFullTypeName; Sender = persistentMessage.Sender; Size = message.Size; Tags = tags}
                                plugin.Serialization.Serialize persistentMessage.Payload (Some eventType) eventMetadata)
                            let expectedVersion =
                                let sequenceNumber = message.LowestSequenceNr - 1L
                                if sequenceNumber = 0L
                                then ExpectedVersion.NoStream |> int64
                                else sequenceNumber - 1L
                            eventStore.AppendToStreamAsync(message.PersistenceId, expectedVersion, plugin.Credentials, events |> Seq.toArray))
                        |> Seq.toArray            
            try 
                let! results = Task.WhenAll(tasks)
                return null
            with | ex ->
                let errors = [ex]@(tasks |> Array.filter (fun task -> task.IsFaulted) |> Array.map (fun task -> task.Exception) |> Seq.cast<exn> |> Seq.toList)
                return ImmutableList.CreateRange(errors) :> IImmutableList<exn>
        }

    override this.DeleteMessagesToAsync (persistenceId, sequenceNumber) =
        task {
            let! eventStore = connect()
            let! metadataResult = eventStore.GetStreamMetadataAsync(persistenceId, plugin.Credentials)
            let metadata = metadataResult.StreamMetadata
            let newMetadata = StreamMetadata.Create(metadata.MaxCount, metadata.MaxAge, sequenceNumber |> Nullable, metadata.CacheControl, metadata.Acl)
            return! eventStore.SetStreamMetadataAsync(persistenceId, metadataResult.MetastreamVersion, newMetadata, plugin.Credentials)
        } :> Task

    override this.ReadHighestSequenceNrAsync (persistenceId, from) =
        task {
            let! eventStore = connect()
            let! eventResult = eventStore.ReadEventAsync(persistenceId, StreamPosition.End |> int64, true, plugin.Credentials)
            match eventResult.Status with
            | EventReadStatus.Success -> return if eventResult.Event.HasValue
                                                then if eventResult.Event.Value.Event |> isNotNull
                                                     then eventResult.Event.Value.Event.EventNumber
                                                     else eventResult.Event.Value.OriginalEventNumber
                                                else eventResult.EventNumber
                                                + 1L
            | EventReadStatus.NotFound ->
                let! streamMetadata = eventStore.GetStreamMetadataAsync(persistenceId, plugin.Credentials) 
                return streamMetadata.StreamMetadata.TruncateBefore.GetValueOrDefault()
            | _ -> return 0L
        }
   
    override this.ReplayMessagesAsync (context, persistenceId, first, last, max, recoveryCallback) =
        task {
            let! eventStore = connect()
            let stopped = AsyncManualResetEvent(initialState = false)
            let start = Math.Max(0L, first - 2L)
            let eventsToRead = Math.Min(last - start + 1L, max)
            let settings = CatchUpSubscriptionSettings(CatchUpSubscriptionSettings.Default.MaxLiveQueueSize, !readBatchSize, false, true)
            let messagesReplayed = ref 0L
            let stop (subscription: EventStoreCatchUpSubscription) =
                subscription.Stop()
                stopped.Set()
            let toPersistentRepresentation (resolvedEvent: ResolvedEvent) =
                let deserializedObject = plugin.Serialization.Deserialize<obj> resolvedEvent.Event 
                let metadata = resolvedEvent.Event.Metadata |> Serialization.parseJsonBytes<EventMetadata>
                let persistent = Akka.Persistence.Persistent(deserializedObject, resolvedEvent.Event.EventNumber + 1L, resolvedEvent.Event.EventStreamId, metadata.EventType, false, metadata.Sender)
                persistent :> IPersistentRepresentation
            let sendMessage subscription (event: ResolvedEvent) =
                try let persistentEvent = event |> toPersistentRepresentation
                    persistentEvent |> recoveryCallback.Invoke
                with | ex -> context.System.Log.Error(ex, sprintf "Error Applying Recovered %s Event from EventStore for %s" event.Event.EventType persistenceId)
                if event.OriginalEventNumber + 1L >= last || Interlocked.Increment(messagesReplayed) > max
                then stop subscription
            let subscription = eventStore.SubscribeToStreamFrom(persistenceId, start |> Nullable, settings, 
                                                                (fun subscription event -> sendMessage subscription event), 
                                                                userCredentials = plugin.Credentials)
            return! stopped.WaitAsync() |> Task.ofUnit
        } :> Task
    