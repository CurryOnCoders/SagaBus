namespace Akka.Persistence.EventStore

open Akka.Actor
open Akka.Configuration
open Akka.Persistence
open Akka.Persistence.EventStore
open Akka.Persistence.Journal
open Akka.Persistence.Query
open Akka.Streams
open Akka.Streams.Dsl
open CurryOn.Akka
open CurryOn.Akka.Serialization
open CurryOn.Common
open EventStore.ClientAPI
open FSharp.Control
open Microsoft.VisualStudio.Threading
open Reactive.Streams
open System
open System.Collections.Immutable
open System.Threading
open System.Threading.Tasks

module EventJournal =
    let get (config: Config) (system: ActorSystem) =
        let plugin = EventStorePlugin(system)
        let eventStore = plugin.Connect() |> Task.runSynchronously
        let readBatchSize = config.GetInt("read-batch-size", 4095)
        let getSnapshotStream persistenceId = sprintf "snapshots-%s" persistenceId
        let catchUpSettings = 
            let def = CatchUpSubscriptionSettings.Default
            CatchUpSubscriptionSettings(def.MaxLiveQueueSize, readBatchSize, def.VerboseLogging, true) 
        let serializeSnapshotMetadata (snapshot: JournalSnapshot) =
            let metadata = {PersistenceId = snapshot.PersistenceId; SequenceNumber = snapshot.SequenceNumber; Timestamp = snapshot.Timestamp}
            metadata |> toJsonBytes
        let deserializeSnapshotMetadata (resolvedEvent: ResolvedEvent) = 
            resolvedEvent.Event.Metadata |> parseJsonBytes<SnapshotMetadata>
        let serializeEvent (event: JournaledEvent) = 
            let eventMetadata = {SequenceNumber = event.SequenceNumber; EventType = event.Manifest; Sender = event.Sender; Tags = event.Tags}
            EventData(Guid.NewGuid(), event.Manifest, true, event.Event |> box |> toJsonBytes, eventMetadata |> toJsonBytes)
        let deserializeEvent (resolvedEvent: ResolvedEvent) =
            let metadata = resolvedEvent.Event.Metadata |> parseJsonBytes<EventMetadata>
            let event = resolvedEvent.Event.Data |> parseJsonBytes<obj>
            (event, metadata)
        let getJournaledEvent resolvedEvent =
            let event, metadata = deserializeEvent resolvedEvent
            { PersistenceId = resolvedEvent.Event.EventStreamId
              SequenceNumber = metadata.SequenceNumber
              Sender = metadata.Sender
              Manifest = metadata.EventType 
              WriterId = Guid.NewGuid() |> string
              Event = event
              Tags = metadata.Tags
            }

        let rehydrateEvent persistenceId eventNumber (metadata: EventMetadata) (event: obj) =
            let persistent = Persistent(event, eventNumber + 1L, persistenceId, metadata.EventType, false, metadata.Sender)
            persistent :> IPersistentRepresentation

        let toPersistentRepresentation (resolvedEvent: ResolvedEvent) =
            let event, metadata = deserializeEvent resolvedEvent
            rehydrateEvent resolvedEvent.Event.EventStreamId resolvedEvent.Event.EventNumber metadata event

        let writeSnapshot (snapshot: JournalSnapshot) =
            operation {
                let snapshotMetadata = snapshot |> serializeSnapshotMetadata
                let eventData = EventData(Guid.NewGuid(), snapshot.Manifest, true, snapshot.Snapshot |> box |> toJsonBytes, snapshotMetadata) 
                return! eventStore.AppendToStreamAsync(getSnapshotStream snapshot.PersistenceId, ExpectedVersion.Any |> int64, plugin.Credentials, eventData)
            }

        let rec findSnapshotMetadata (criteria: SnapshotSelectionCriteria) persistenceId startIndex =
            operation {
                let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(getSnapshotStream persistenceId, startIndex, readBatchSize, true, userCredentials = plugin.Credentials)
                let metadataFound = 
                    eventSlice.Events
                    |> Seq.map (fun event -> (event, deserializeSnapshotMetadata event))
                    |> Seq.tryFind (fun (event, metadata) -> metadata.SequenceNumber <= criteria.MaxSequenceNr && metadata.Timestamp <= criteria.MaxTimeStamp)
                match metadataFound with
                | Some (event, metadata) -> return (metadata, event.Event.Data |> parseJsonBytes<obj>) |> Some
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
                return! findSnapshotMetadata criteria persistenceId -1L (*end*)            
            }

        {new IStreamingEventJournal with
            member __.GetCurrentPersistenceIds () =
                operation {
                    let rec readSlice startPosition ids =
                        task {
                            let! eventSlice = eventStore.ReadStreamEventsForwardAsync("$streams", startPosition, readBatchSize, true, userCredentials = plugin.Credentials)
                            let newIds = 
                                eventSlice.Events 
                                |> Seq.filter (fun resolved -> resolved.Event |> isNotNull)
                                |> Seq.map (fun resolved -> resolved.Event.EventStreamId) 
                                |> Seq.filter (fun stream -> stream.StartsWith("snapshots") |> not)
                                |> Seq.fold (fun acc cur -> acc |> Set.add cur) ids
                            if eventSlice.IsEndOfStream |> not
                            then return! newIds |> readSlice eventSlice.NextEventNumber
                            else return newIds
                        }
                    let! persistenceIds = Set.empty<string> |> readSlice 0L
                    return! Result.success persistenceIds
                }
            member __.PersistEvents eventsToPersist =
                operation {
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
                        
                            let eventSet = events |> Seq.map serializeEvent |> Seq.toArray

                            eventStore.AppendToStreamAsync(persistenceId, expectedVersion, plugin.Credentials, eventSet))                           
                        |> Task.Parallel

                    return! Result.successWithEvents () [PersistedSuccessfully]
                }
            member __.DeleteEvents persistenceId upperLimit =
                operation {
                    let! metadataResult = eventStore.GetStreamMetadataAsync(persistenceId, plugin.Credentials)
                    let metadata = metadataResult.StreamMetadata
                    let newMetadata = StreamMetadata.Create(metadata.MaxCount, metadata.MaxAge, upperLimit |> Nullable, metadata.CacheControl, metadata.Acl)
                    let! result = eventStore.SetStreamMetadataAsync(persistenceId, metadataResult.MetastreamVersion, newMetadata, plugin.Credentials)
                    return! Result.successWithEvents () [DeletedSuccessfully]
                }
            member __.GetMaxSequenceNumber persistenceId from =
                operation {
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
                    return! Result.success (events |> List.rev |> Seq.ofList)
                }
            member __.GetTaggedEvents tag lowOffset highOffset =
                operation {
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
                                |> Seq.map (fun resolvedEvent -> resolvedEvent, deserializeEvent resolvedEvent)
                                |> Seq.filter (fun (_,(_,metadata)) -> metadata.Tags |> Seq.contains tag)
                                |> Seq.map (fun (resolved,(event,metadata)) -> {Id = 0L; Tag = tag; Event = event |> rehydrateEvent resolved.Event.EventStreamId resolved.Event.EventNumber metadata})
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
                    let stream = getSnapshotStream persistenceId
                    let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(stream, -1L, readBatchSize, true, userCredentials = plugin.Credentials)

                    let upperLimit =
                        eventSlice.Events
                        |> Seq.map (fun event -> event, deserializeSnapshotMetadata event)
                        |> Seq.filter (fun (_,metadata) -> metadata.SequenceNumber >= criteria.MinSequenceNr && metadata.SequenceNumber <= criteria.MaxSequenceNr && metadata.Timestamp >= criteria.MinTimestamp.GetValueOrDefault() && metadata.Timestamp = criteria.MaxTimeStamp)
                        |> Seq.maxBy (fun (event,_) -> event.Event.EventNumber)
                        |> fun (event,_) -> event.Event.EventNumber

                    let! metadataResult = eventStore.GetStreamMetadataAsync(stream, plugin.Credentials)
                    let metadata = metadataResult.StreamMetadata
                    let newMetadata = StreamMetadata.Create(metadata.MaxCount, metadata.MaxAge, upperLimit |> Nullable, metadata.CacheControl, metadata.Acl)
                    let! result = eventStore.SetStreamMetadataAsync(stream, metadataResult.MetastreamVersion, newMetadata, plugin.Credentials)

                    return! Result.successWithEvents () [DeletedSuccessfully]
                }
            member __.DeleteAllSnapshots persistenceId sequenceNumber =
                operation {
                    let stream = getSnapshotStream persistenceId
                    let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(stream, -1L, readBatchSize, true, userCredentials = plugin.Credentials)

                    let upperLimit =
                        eventSlice.Events
                        |> Seq.map (fun event -> event, deserializeSnapshotMetadata event)
                        |> Seq.filter (fun (_,metadata) -> metadata.SequenceNumber <= sequenceNumber)
                        |> Seq.maxBy (fun (event,_) -> event.Event.EventNumber)
                        |> fun (event,_) -> event.Event.EventNumber

                    let! metadataResult = eventStore.GetStreamMetadataAsync(stream, plugin.Credentials)
                    let metadata = metadataResult.StreamMetadata
                    let newMetadata = StreamMetadata.Create(metadata.MaxCount, metadata.MaxAge, upperLimit |> Nullable, metadata.CacheControl, metadata.Acl)
                    let! result = eventStore.SetStreamMetadataAsync(stream, metadataResult.MetastreamVersion, newMetadata, plugin.Credentials)

                    return! Result.successWithEvents () [DeletedSuccessfully]
                }
            member __.SubscribeToEvents persistenceId fromSequence =
                {new IPublisher<JournaledEvent> with
                    member __.Subscribe subscriber = 
                        eventStore.SubscribeToStreamFrom(persistenceId, fromSequence |> Nullable, catchUpSettings, (fun _ event -> event |> getJournaledEvent |> subscriber.OnNext), userCredentials = plugin.Credentials)
                        |> ignore
                }
            member __.SubscribeToPersistenceIds () =
                {new IPublisher<string> with
                    member __.Subscribe subscriber = 
                        let notifyEvent (event: ResolvedEvent) =
                            if event.Event |> isNotNull
                            then event.Event.EventStreamId |> subscriber.OnNext

                        eventStore.SubscribeToStreamFrom("$streams", Nullable 0L, catchUpSettings, (fun _ event -> notifyEvent event), userCredentials = plugin.Credentials)
                        |> ignore
                }
        }

type EventStoreProvider() =
    interface IStreamingEventJournalProvider with
        member __.GetEventJournal config context = EventJournal.get config context.System :> IEventJournal
        member __.GetStreamingEventJournal config system = EventJournal.get config system

type EventStoreJournal (config: Config) =
    inherit StreamingEventJournal<EventStoreProvider>(config)
    static member Identifier = "akka.persistence.journal.event-store"

type EventStoreSnapshotStore (config: Config) =
    inherit SnapshotStoreBase<EventStoreProvider>(config)
    static member Identifier = "akka.persistence.snapshot-store.event-store"