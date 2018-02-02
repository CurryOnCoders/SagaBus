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
open Akka.Persistence.Query

module EventJournal =
    let get (config: Config) (context: IActorContext) =
        let plugin = EventStorePlugin(context)
        let connect () = plugin.Connect () 
        let readBatchSize = config.GetInt("read-batch-size", 4095)
        let getMetadataStream persistenceId = sprintf "snapshots-%s" persistenceId
        let getStream persistenceId version = sprintf "snapshot-%s-%d" persistenceId version
        let toMetadata (resolvedEvent: ResolvedEvent) = resolvedEvent.Event.Data |> Serialization.parseJsonBytes<SnapshotMetadata>

        let toPersistentRepresentation (resolvedEvent: ResolvedEvent) =
            let deserializedObject = plugin.Serialization.Deserialize<obj> resolvedEvent.Event 
            let metadata = resolvedEvent.Event.Metadata |> Serialization.parseJsonBytes<EventMetadata>
            let persistent = Persistent(deserializedObject, resolvedEvent.Event.EventNumber + 1L, resolvedEvent.Event.EventStreamId, metadata.EventType, false, metadata.Sender)
            persistent :> IPersistentRepresentation

        let addSnapshotToMetadataLog (snapshot: JournalSnapshot) =
            operation {
                let! eventStore = connect()
                let logStream = getMetadataStream snapshot.PersistenceId
                let eventMetadata = {PersistenceId = snapshot.PersistenceId; SequenceNumber = snapshot.SequenceNumber; Timestamp = snapshot.Timestamp} |> Serialization.toJsonBytes
                let eventData = EventData(Guid.NewGuid(), typeof<SnapshotMetadata>.Name, true, eventMetadata, [||])
                let! writeResult = eventStore.AppendToStreamAsync(logStream, ExpectedVersion.Any |> int64, plugin.Credentials, eventData)
                return eventMetadata
            }

        let writeSnapshot (snapshot: JournalSnapshot) =
            operation {
                let! eventStore = connect()
                let! eventMetadata = addSnapshotToMetadataLog snapshot
                let eventData = EventData(Guid.NewGuid(), snapshot.Manifest, true, snapshot.Snapshot |> Serialization.toJsonBytes, eventMetadata) 
                return! eventStore.AppendToStreamAsync(getStream snapshot.PersistenceId snapshot.SequenceNumber, ExpectedVersion.Any |> int64, plugin.Credentials, eventData)
            }

        let rec findSnapshotMetadata (criteria: SnapshotSelectionCriteria) persistenceId startIndex =
            operation {
                let! eventStore = connect()
                let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(getMetadataStream persistenceId, startIndex, readBatchSize, true, userCredentials = plugin.Credentials)
                let metadataFound = 
                    eventSlice.Events
                    |> Seq.map toMetadata
                    |> Seq.tryFind (fun metadata -> metadata.SequenceNumber <= criteria.MaxSequenceNr && metadata.Timestamp <= criteria.MaxTimeStamp)
                match metadataFound with
                | Some metadata -> return metadata |> Some
                | None -> 
                    let lastEvent = if eventSlice.Events |> isNull || eventSlice.Events.Length = 0
                                    then -1L
                                    else (eventSlice.Events |> Seq.last |> fun e -> e.Event.EventNumber)
                    if lastEvent <= 0L
                    then return None
                    else return! findSnapshotMetadata criteria persistenceId lastEvent
            }

        let findSnapshot criteria persistenceId =
            operation {
                let! eventStore = connect()
                let! snapshotMetadata = findSnapshotMetadata criteria persistenceId -1L (*end*)
                match snapshotMetadata with
                | Some metadata ->
                    let snapshotStream = getStream metadata.PersistenceId metadata.SequenceNumber
                    let! snapshotReadResult = eventStore.ReadEventAsync(snapshotStream, StreamPosition.End |> int64, true, userCredentials = plugin.Credentials) 
                    let snapshotEvent = snapshotReadResult.Event.Value
                    return (metadata, Serialization.parseJsonBytes<obj> snapshotEvent.Event.Data) |> Some
                | None -> return None            
            }

        {new  IEventJournal with
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
            member __.PersistEvents eventsToPersist =
                operation {
                    let! eventStore = connect()
                    let! result =
                        eventsToPersist 
                        |> Seq.groupBy (fun event -> event.PersistenceId)
                        |> Seq.map (fun (persistenceId, events) ->                        
                            let expectedVersion =
                                let sequenceNumber = 
                                    if events |> Seq.isEmpty
                                    then 0L
                                    else events |> Seq.map (fun event -> event.SequenceNumber) |> Seq.min
                                if sequenceNumber <= 1L
                                then ExpectedVersion.NoStream |> int64
                                else sequenceNumber - 2L
                        
                            let eventSet = 
                                events 
                                |> Seq.map (fun journalEvent ->
                                    let eventMetadata = {EventType = journalEvent.Manifest; Sender = journalEvent.Sender; Tags = journalEvent.Tags}
                                    plugin.Serialization.Serialize journalEvent.Event (Some journalEvent.Manifest) eventMetadata)
                                |> Seq.toArray

                            eventStore.AppendToStreamAsync(persistenceId, expectedVersion, plugin.Credentials, eventSet))                           
                        |> Task.Parallel

                    return! Result.successWithEvents () [PersistedSuccessfully]
                }
            member __.DeleteEvents persistenceId upperLimit =
                operation {
                    let! eventStore = connect()
                    let! metadataResult = eventStore.GetStreamMetadataAsync(persistenceId, plugin.Credentials)
                    let metadata = metadataResult.StreamMetadata
                    let newMetadata = StreamMetadata.Create(metadata.MaxCount, metadata.MaxAge, upperLimit |> Nullable, metadata.CacheControl, metadata.Acl)
                    let! result = eventStore.SetStreamMetadataAsync(persistenceId, metadataResult.MetastreamVersion, newMetadata, plugin.Credentials)
                    return! Result.successWithEvents () [DeletedSuccessfully]
                }
            member __.GetMaxSequenceNumber persistenceId from =
                operation {
                    let! eventStore = connect()
                    let! eventResult = eventStore.ReadEventAsync(persistenceId, StreamPosition.End |> int64, true, plugin.Credentials)
                    match eventResult.Status with
                    | EventReadStatus.Success -> 
                        return! if eventResult.Event.HasValue
                                then if eventResult.Event.Value.Event |> isNotNull
                                        then eventResult.Event.Value.Event.EventNumber
                                        else eventResult.Event.Value.OriginalEventNumber
                                else eventResult.EventNumber
                                + 1L |> Some |> Result.success
                    | EventReadStatus.NotFound ->
                        let! streamMetadata = eventStore.GetStreamMetadataAsync(persistenceId, plugin.Credentials) 
                        return! streamMetadata.StreamMetadata.TruncateBefore.GetValueOrDefault() |> Some |> Result.success
                    | _ -> return! Result.success None                
                }
            member __.GetEvents persistenceId first last max =
                operation {
                    let! eventStore = connect()
                    let stopped = AsyncManualResetEvent(initialState = false)
                    let start = Math.Max(0L, first - 2L)
                    let eventsToRead = Math.Min(last - start + 1L, max)

                    let rec getEvents offset eventsSoFar =
                        task {
                            let! slice = eventStore.ReadStreamEventsForwardAsync(persistenceId, start, eventsToRead |> int, true, plugin.Credentials)
                            let events = slice.Events |> Seq.map toPersistentRepresentation |> Seq.toList |> List.fold (fun acc cur -> cur::acc) eventsSoFar
                        
                            if slice.IsEndOfStream
                            then return events
                            else return! getEvents slice.NextEventNumber events
                        }

                    let! events = getEvents start []
                    return! Result.success (events |> Seq.ofList)
                }
            member __.GetTaggedEvents tag lowOffset highOffset =
                operation {
                    let! eventStore = plugin.Connect()
                    let position = 
                        match lowOffset with
                        | Some offset -> if offset = 0L then Position.Start else Position(offset, offset)
                        | None -> Position.Start

                    let rec readSlice startPosition eventSoFar =
                        task {
                            let! eventSlice = eventStore.ReadAllEventsForwardAsync(startPosition, readBatchSize, true, userCredentials = plugin.Credentials)
                            let events = 
                                eventSlice.Events 
                                |> Seq.filter (fun resolvedEvent -> resolvedEvent.Event |> isNotNull)
                                |> Seq.filter (fun resolvedEvent ->
                                    try let metadata = resolvedEvent.Event.Metadata |> Serialization.parseJsonBytes<EventMetadata> 
                                        metadata.Tags |> Seq.contains tag
                                    with | _ -> false)
                                |> Seq.map (fun resolvedEvent -> {Id = 0L; Tag = tag; Event = resolvedEvent |> toPersistentRepresentation})
                                |> Seq.fold (fun acc cur -> seq { yield! acc; yield cur }) eventSoFar
                            if eventSlice.IsEndOfStream |> not
                            then return! readSlice eventSlice.NextPosition events
                            else return events
                        }

                    let! events = readSlice Position.Start Seq.empty
                    return! Result.success events
                }
            member __.SaveSnapshot snapshot =
                operation {            
                    let! result = writeSnapshot snapshot
                    return! Result.successWithEvents () [PersistedSuccessfully]
                }
            member __.GetSnapshot persistenceId criteria =
                operation {
                    try
                        let! metadataResult = findSnapshot criteria persistenceId
                        return!
                            match metadataResult with
                            | Some (metadata, snapshot) ->
                                 { PersistenceId = persistenceId; 
                                   Manifest = snapshot.GetType().FullName; 
                                   SequenceNumber = metadata.SequenceNumber; 
                                   Timestamp = metadata.Timestamp; 
                                   Snapshot = snapshot
                                 } |> Some
                            | None -> None                            
                            |> Result.success
                    with | _ -> 
                        return! None |> Result.success
                }
            member __.DeleteSnapshots persistenceId criteria =
                operation {
                    let! eventStore = connect()
                    let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(getMetadataStream persistenceId, -1L, readBatchSize, true, userCredentials = plugin.Credentials)

                    let! result =
                        eventSlice.Events
                        |> Seq.map toMetadata
                        |> Seq.filter (fun metadata -> metadata.SequenceNumber >= criteria.MinSequenceNr && metadata.SequenceNumber <= criteria.MaxSequenceNr && metadata.Timestamp >= criteria.MinTimestamp.GetValueOrDefault() && metadata.Timestamp = criteria.MaxTimeStamp)
                        |> Seq.map (fun metadata -> eventStore.DeleteStreamAsync(getStream metadata.PersistenceId metadata.SequenceNumber, ExpectedVersion.Any |> int64, userCredentials = plugin.Credentials))
                        |> Task.Parallel

                    return! Result.successWithEvents () [DeletedSuccessfully]
                }
            member __.DeleteAllSnapshots persistenceId sequenceNumber =
                operation {
                    let! eventStore = connect()
                    let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(getMetadataStream persistenceId, -1L, readBatchSize, true, userCredentials = plugin.Credentials)

                    let! result = 
                        eventSlice.Events
                        |> Seq.map toMetadata
                        |> Seq.filter (fun snapshotMetadata -> snapshotMetadata.SequenceNumber <= sequenceNumber)
                        |> Seq.map (fun snapshotMetadata -> eventStore.DeleteStreamAsync(getStream snapshotMetadata.PersistenceId snapshotMetadata.SequenceNumber, ExpectedVersion.Any |> int64, userCredentials = plugin.Credentials))
                        |> Task.Parallel

                    return! Result.successWithEvents () [DeletedSuccessfully]
                }
        }

type EventStoreProvider() =
    interface IEventJournalProvider with
        member __.GetEventJournal config context = EventJournal.get config context

type EventStoreJournal (config: Config) =
    inherit StreamingEventJournal<EventStoreProvider>(config)
    static member Identifier = "akka.persistence.journal.event-store"

type EventStoreSnapshotStore (config: Config) =
    inherit StreamingSnapshotStore<EventStoreProvider>(config)
    static member Identifier = "akka.persistence.snapshot-store.event-store"