namespace Akka.Persistence.EventStore

open Akka.Actor
open Akka.Persistence
open Akka.Persistence.EventStore
open Akka.Persistence.Query
open Akka.Streams.Dsl
open CurryOn.Common
open EventStore.ClientAPI
open Reactive.Streams
open System

module internal EventJournal =
    let searchForType = memoize <| Types.findType        

    let getEventType (resolvedEvent: ResolvedEvent) =
        let eventType = resolvedEvent.Event.EventType
        match searchForType eventType with
        | Success clrType -> clrType
        | Failure ex -> raise ex

    let deserialize (serialization: EventStoreSerialization) (eventType: Type) (event: RecordedEvent) =
        let deserializer = serialization.GetType().GetMethod("Deserialize").MakeGenericMethod(eventType)
        deserializer.Invoke(serialization, [|event|])

    let deserializeEvent (serialization: EventStoreSerialization) (resolvedEvent: ResolvedEvent) =
        let eventType = resolvedEvent |> getEventType
        resolvedEvent.Event |> deserialize serialization eventType

type EventStoreReadJournal (system: ExtendedActorSystem) =
    let serialization = EventStoreSerialization(system)
    let deserialize = EventJournal.deserializeEvent serialization
    let plugin = EventStorePlugin(system)
    let defaultSettings = CatchUpSubscriptionSettings.Default
    let config = lazy(system.Settings.Config.GetConfig("akka.persistence.journal.event-store"))
    let readBatchSize = lazy(config.Value.GetInt("read-batch-size"))
    static member Identifier = "eventstore.persistence.query"
    interface IReadJournal
    interface IAllPersistenceIdsQuery with
        member __.AllPersistenceIds () =
            Source.FromPublisher 
                {new IPublisher<string> with
                    member __.Subscribe subscriber =
                        task {
                            let! eventStore = plugin.Connect()
                            let notify (resolvedEvent: ResolvedEvent) =
                                if resolvedEvent.Event |> isNotNull
                                then subscriber.OnNext(resolvedEvent.Event.EventStreamId)
                            return eventStore.SubscribeToStreamFrom("$streams", StreamPosition.Start |> int64 |> Nullable, defaultSettings, (fun _ event -> notify event), userCredentials = plugin.Credentials) |> ignore
                        } |> Task.runSynchronously
                }
    interface ICurrentEventsByPersistenceIdQuery with
        member __.CurrentEventsByPersistenceId (persistenceId, fromSequence, toSequence) =
            Source.FromPublisher
                {new IPublisher<EventEnvelope> with
                    member __.Subscribe subscriber =
                        task {
                            let! eventStore = plugin.Connect()
                            let rec readSlice startPosition =
                                task {
                                    let! eventSlice = eventStore.ReadStreamEventsForwardAsync(persistenceId, startPosition, Math.Min(readBatchSize.Value, (toSequence - startPosition) |> int), true, userCredentials = plugin.Credentials)
                                    eventSlice.Events |> Seq.filter (fun resolvedEvent -> resolvedEvent.Event |> isNotNull)
                                                      |> Seq.iter (fun resolvedEvent -> 
                                                            try let event = resolvedEvent |> deserialize 
                                                                EventEnvelope(0L, persistenceId, resolvedEvent.Event.EventNumber + 1L, event) |> subscriber.OnNext
                                                            with | ex -> subscriber.OnError ex)
                                    if eventSlice.IsEndOfStream |> not && eventSlice.NextEventNumber < toSequence
                                    then return! readSlice eventSlice.NextEventNumber
                                }
                            do! readSlice fromSequence
                            subscriber.OnComplete()
                        } |> Task.runSynchronously
                }
    interface ICurrentEventsByTagQuery with
        member __.CurrentEventsByTag (tag, offset) =
            Source.FromPublisher 
                {new IPublisher<EventEnvelope> with
                    member __.Subscribe subscriber =
                        task {
                            let! eventStore = plugin.Connect()
                            let position = if offset = 0L then Position.Start else Position(offset, offset)

                            let rec readSlice startPosition =
                                task {
                                    let! eventSlice = eventStore.ReadAllEventsForwardAsync(startPosition, readBatchSize.Value, true, userCredentials = plugin.Credentials)
                                    eventSlice.Events |> Seq.filter (fun resolvedEvent -> resolvedEvent.Event |> isNotNull)
                                                      |> Seq.iter (fun resolvedEvent -> 
                                                            try let metadata = resolvedEvent.Event.Metadata |> Serialization.parseJsonBytes<EventMetadata>
                                                                if metadata.Tags |> Seq.contains tag
                                                                then let event = resolvedEvent |> deserialize
                                                                     EventEnvelope(offset, resolvedEvent.Event.EventStreamId, resolvedEvent.Event.EventNumber + 1L, event) |> subscriber.OnNext
                                                            with | ex -> subscriber.OnError ex)
                                    if eventSlice.IsEndOfStream |> not
                                    then return! readSlice eventSlice.NextPosition
                                }
                            
                            do! readSlice position
                            subscriber.OnComplete()
                        } |> Task.runSynchronously
                }
    interface ICurrentPersistenceIdsQuery with
        member __.CurrentPersistenceIds () =
            Source.FromPublisher
                {new IPublisher<string> with
                    member __.Subscribe subscriber =
                        task {
                            let! eventStore = plugin.Connect()
                            let rec readSlice startPosition =
                                task {
                                    let! eventSlice = eventStore.ReadStreamEventsForwardAsync("$streams", startPosition, readBatchSize.Value, false, userCredentials = plugin.Credentials)
                                    eventSlice.Events |> Seq.map (fun event -> event.OriginalStreamId) |> Seq.iter subscriber.OnNext
                                    if eventSlice.IsEndOfStream |> not
                                    then return! readSlice eventSlice.NextEventNumber
                                }
                            do! readSlice (StreamPosition.Start |> int64)
                            subscriber.OnComplete()
                        } |> Task.runSynchronously
                }
    interface IEventsByPersistenceIdQuery with
        member __.EventsByPersistenceId (persistenceId, fromSequence, toSequence) =
            Source.FromPublisher
                {new IPublisher<EventEnvelope> with
                    member __.Subscribe subscriber =
                        task {
                            let! eventStore = plugin.Connect()
                            let settings = CatchUpSubscriptionSettings(defaultSettings.MaxLiveQueueSize, readBatchSize.Value, false, true)
                            let handleEvent stopSubscription (resolvedEvent: ResolvedEvent) =
                                if resolvedEvent.Event |> isNotNull
                                then let event = resolvedEvent.Event
                                     try EventEnvelope(0L, persistenceId, event.EventNumber, resolvedEvent |> deserialize) |> subscriber.OnNext
                                     with | ex -> subscriber.OnError ex
                                     if event.EventNumber >= toSequence
                                     then stopSubscription ()
                                          subscriber.OnComplete()                        
                            return 
                                if fromSequence < 0L || fromSequence = Int64.MaxValue
                                then eventStore.SubscribeToStreamAsync(persistenceId, true, (fun subscription event -> event |> handleEvent subscription.Unsubscribe), userCredentials = plugin.Credentials) |> Task.ignoreSynchronously
                                else eventStore.SubscribeToStreamFrom(persistenceId, Nullable fromSequence, settings, (fun subscription event -> event |> handleEvent subscription.Stop), userCredentials = plugin.Credentials) |> ignore
                        } |> Task.runSynchronously
                }
    interface IEventsByTagQuery with
        member __.EventsByTag (tag, offset) =  
            Source.FromPublisher
                {new IPublisher<EventEnvelope> with
                    member __.Subscribe subscriber =
                        task {
                            let! eventStore = plugin.Connect()
                            let settings = CatchUpSubscriptionSettings(defaultSettings.MaxLiveQueueSize, defaultSettings.ReadBatchSize, false, true)
                            let handleEvent stopSubscription (resolvedEvent: ResolvedEvent) =
                                if resolvedEvent.Event |> isNotNull
                                then let event = resolvedEvent.Event
                                     try let metadata = resolvedEvent.Event.Metadata |> Serialization.parseJsonBytes<EventMetadata>
                                         if metadata.Tags |> Seq.contains tag 
                                         then EventEnvelope(0L, event.EventStreamId, event.EventNumber, resolvedEvent |> deserialize) |> subscriber.OnNext
                                     with | ex -> subscriber.OnError ex                  
                            return
                                if offset < 0L || offset = Int64.MaxValue
                                then eventStore.SubscribeToAllAsync(true, (fun subscription event -> event |> handleEvent subscription.Unsubscribe), userCredentials = plugin.Credentials) |> Task.ignoreSynchronously
                                else eventStore.SubscribeToAllFrom(Nullable <| Position(offset,offset), settings, (fun subscription event -> event |> handleEvent subscription.Stop), userCredentials = plugin.Credentials) |> ignore
                        } |> Task.runSynchronously
                }


type EventStoreReadJournalProvider (system: ExtendedActorSystem) =
    interface IReadJournalProvider with
        member __.GetReadJournal () = EventStoreReadJournal(system) :> IReadJournal

