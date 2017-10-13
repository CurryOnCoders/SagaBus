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
    let searchForType = memoize <| fun (typeName: string) ->
        AppDomain.CurrentDomain.GetAssemblies()
        |> Seq.collect (fun assembly -> try assembly.GetTypes() with | _ -> [||])
        |> Seq.find (fun clrType -> 
            if typeName.Contains(".")
            then let typeNamespace = typeName.Substring(0, typeName.LastIndexOf('.'))
                 let name = typeName.Substring(typeName.LastIndexOf('.') + 1)
                 clrType.Namespace = typeNamespace && clrType.Name = name
            else clrType.Name = typeName)

    let getEventType (resolvedEvent: ResolvedEvent) =
        let eventType = resolvedEvent.Event.EventType
        try Type.GetType(eventType)
        with | _ -> searchForType eventType

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
    static member Identifier = "eventstore.persistence.query"
    interface IReadJournal
    interface IAllPersistenceIdsQuery with
        member __.AllPersistenceIds () =
            let eventStore = plugin.Connect()
            Source.FromPublisher 
                {new IPublisher<string> with
                    member __.Subscribe subscriber =
                        let notify (resolvedEvent: ResolvedEvent) =
                            if resolvedEvent.Event |> isNotNull
                            then subscriber.OnNext(resolvedEvent.Event.EventStreamId)
                        eventStore.SubscribeToStreamFrom("$streams", 0L |> Nullable, defaultSettings, (fun _ event -> notify event), userCredentials = plugin.Credentials) |> ignore
                }
    interface ICurrentEventsByPersistenceIdQuery with
        member __.CurrentEventsByPersistenceId (persistenceId, fromSequence, toSequence) =
            let eventStore = plugin.Connect()
            Source.FromPublisher
                {new IPublisher<EventEnvelope> with
                    member __.Subscribe subscriber =
                        task {
                            let! eventSlice = eventStore.ReadStreamEventsForwardAsync(persistenceId, fromSequence, (toSequence - fromSequence |> int), true, userCredentials = plugin.Credentials)
                            eventSlice.Events |> Seq.filter (fun resolvedEvent -> resolvedEvent.Event |> isNotNull)
                                              |> Seq.iter (fun resolvedEvent -> 
                                                    try let event = resolvedEvent |> deserialize 
                                                        EventEnvelope(0L, persistenceId, resolvedEvent.Event.EventNumber + 1L, event) |> subscriber.OnNext
                                                    with | ex -> subscriber.OnError ex)
                            subscriber.OnComplete()
                        } |> Task.runSynchronously
                }
    interface ICurrentEventsByTagQuery with
        member __.CurrentEventsByTag (tag, offset) =
            let eventStore = plugin.Connect()
            Source.FromPublisher 
                {new IPublisher<EventEnvelope> with
                    member __.Subscribe subscriber =
                        task {
                            let position = if offset = 0L then Position.Start else Position(offset, offset)
                            let! eventSlice = eventStore.ReadAllEventsForwardAsync(position, Int32.MaxValue, true, userCredentials = plugin.Credentials)
                            eventSlice.Events |> Seq.filter (fun resolvedEvent -> resolvedEvent.Event |> isNotNull)
                                              |> Seq.iter (fun resolvedEvent -> 
                                                    try let metadata = resolvedEvent.Event.Metadata |> Serialization.parseJsonBytes<EventMetadata>
                                                        if metadata.Tags |> Seq.contains tag
                                                        then let event = resolvedEvent |> deserialize
                                                             EventEnvelope(offset, resolvedEvent.Event.EventStreamId, resolvedEvent.Event.EventNumber + 1L, event) |> subscriber.OnNext
                                                    with | ex -> subscriber.OnError ex)
                            subscriber.OnComplete()
                        } |> Task.runSynchronously
                }
    interface ICurrentPersistenceIdsQuery with
        member __.CurrentPersistenceIds () =
            let eventStore = plugin.Connect()
            Source.FromPublisher
                {new IPublisher<string> with
                    member __.Subscribe subscriber =
                        task {
                            let! eventSlice = eventStore.ReadStreamEventsForwardAsync("$streams", 0L, Int32.MaxValue, false, userCredentials = plugin.Credentials)
                            eventSlice.Events |> Seq.map (fun event -> event.OriginalStreamId) |> Seq.iter subscriber.OnNext
                            subscriber.OnComplete()
                        } |> Task.runSynchronously
                }
    interface IEventsByPersistenceIdQuery with
        member __.EventsByPersistenceId (persistenceId, fromSequence, toSequence) =
            let eventStore = plugin.Connect()
            Source.FromPublisher
                {new IPublisher<EventEnvelope> with
                    member __.Subscribe subscriber =
                        let settings = CatchUpSubscriptionSettings(defaultSettings.MaxLiveQueueSize, defaultSettings.ReadBatchSize, false, true)
                        let handleEvent stopSubscription (resolvedEvent: ResolvedEvent) =
                            if resolvedEvent.Event |> isNotNull
                            then let event = resolvedEvent.Event
                                 try EventEnvelope(0L, persistenceId, event.EventNumber, resolvedEvent |> deserialize) |> subscriber.OnNext
                                 with | ex -> subscriber.OnError ex
                                 if event.EventNumber >= toSequence
                                 then stopSubscription ()
                                      subscriber.OnComplete()                        
                        if fromSequence < 0L || fromSequence = Int64.MaxValue
                        then eventStore.SubscribeToStreamAsync(persistenceId, true, (fun subscription event -> event |> handleEvent subscription.Unsubscribe), userCredentials = plugin.Credentials) |> Task.ignoreSynchronously
                        else eventStore.SubscribeToStreamFrom(persistenceId, Nullable fromSequence, settings, (fun subscription event -> event |> handleEvent subscription.Stop), userCredentials = plugin.Credentials) |> ignore
                }
    interface IEventsByTagQuery with
        member __.EventsByTag (tag, offset) =  
            let eventStore = plugin.Connect()
            Source.FromPublisher
                {new IPublisher<EventEnvelope> with
                    member __.Subscribe subscriber =
                        let settings = CatchUpSubscriptionSettings(defaultSettings.MaxLiveQueueSize, defaultSettings.ReadBatchSize, false, true)
                        let handleEvent stopSubscription (resolvedEvent: ResolvedEvent) =
                            if resolvedEvent.Event |> isNotNull
                            then let event = resolvedEvent.Event
                                 try let metadata = resolvedEvent.Event.Metadata |> Serialization.parseJsonBytes<EventMetadata>
                                     if metadata.Tags |> Seq.contains tag 
                                     then EventEnvelope(0L, event.EventStreamId, event.EventNumber, resolvedEvent |> deserialize) |> subscriber.OnNext
                                 with | ex -> subscriber.OnError ex                  
                        if offset < 0L || offset = Int64.MaxValue
                        then eventStore.SubscribeToAllAsync(true, (fun subscription event -> event |> handleEvent subscription.Unsubscribe), userCredentials = plugin.Credentials) |> Task.ignoreSynchronously
                        else eventStore.SubscribeToAllFrom(Nullable <| Position(offset,offset), settings, (fun subscription event -> event |> handleEvent subscription.Stop), userCredentials = plugin.Credentials) |> ignore
                }


type EventStoreReadJournalProvider (system: ExtendedActorSystem) =
    interface IReadJournalProvider with
        member __.GetReadJournal () = EventStoreReadJournal(system) :> IReadJournal

