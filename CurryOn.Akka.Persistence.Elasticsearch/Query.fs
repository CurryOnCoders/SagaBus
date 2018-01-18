namespace Akka.Persistence.Elasticsearch

open Akka.Actor
open Akka.FSharp
open Akka.Persistence
open Akka.Persistence.Query
open Akka.Streams.Dsl
open CurryOn.Common
open CurryOn.Elastic
open FSharp.Control
open Reactive.Streams
open System
open System.Collections.Generic

type ElasticsearchReadJournal (system: ExtendedActorSystem) =
    let serialization = ElasticsearchSerialization(system)
    let plugin = ElasticsearchPlugin(system)
    let config = lazy(system.Settings.Config.GetConfig("akka.persistence.journal.elasticsearch"))
    let readBatchSize = lazy(config.Value.GetInt("read-batch-size"))
    let client = plugin.Connect()
    
    let toOffset (id: DocumentId) =
        match id with
        | IntegerId i -> i
        | _ -> 0L

    let processHits (subscriber: ISubscriber<EventEnvelope>) (search: SearchResult<PersistedEvent>)  =
        search.Results.Hits
        |> List.map (fun hit -> EventEnvelope(hit.Id |> toOffset, hit.Document.PersistenceId, hit.Document.SequenceNumber, hit.Document.Event |> Serialization.parseJson<obj>))
        |> List.iter (fun event -> subscriber.OnNext event)

    let getCurrentPersistenceIds () =
        operation {
            let! distinctValues = client |> Search.distinct<PersistedEvent,string> <@ fun persistedEvent -> persistedEvent.PersistenceId @> None
            return! distinctValues.Aggregations |> List.map (fun value -> value.Key) |> Result.success
        }

    let allPersistenceIdsActor = 
        spawn system "all-persistence-ids"
        <| fun (inbox: AllPersistenceIdsMessages Actor) ->
            let subscribers = new List<ISubscriber<string>>()
            let rec messageLoop () =
                actor {
                    let! message = inbox.Receive()
                    match message with
                    | RegisterSubscriber subscriber -> subscribers.Add(subscriber)
                    | NewPersistenceId persistenceId -> subscribers |> Seq.iter (fun subscriber -> subscriber.OnNext persistenceId)
                    return! messageLoop ()
                }
            messageLoop ()

    static member Identifier = "elasticsearch.persistence.query"

    member __.CurrentEventsByPersistenceId (persistenceId, fromSequence, toSequence) =
        Source.FromPublisher
            {new IPublisher<EventEnvelope> with
                member __.Subscribe subscriber =
                    let result = 
                        operation {
                            let! search =
                                Query.field<PersistedEvent,string> <@ fun persistedEvent -> persistedEvent.PersistenceId @> persistenceId
                                |> Query.And (Query.range<PersistedEvent,int64> <@ fun persistedEvent -> persistedEvent.SequenceNumber @> (Inclusive fromSequence) (Inclusive toSequence))
                                |> Query.execute<PersistedEvent> client None (Some <| Sort.ascending <@ fun persistedEvent -> persistedEvent.SequenceNumber @>) None None

                            search |> processHits subscriber
                        } |> Operation.wait
                    match result with
                    | Failure events -> events |> List.iter (fun event -> 
                        match event.ToException() with
                        | Some error -> subscriber.OnError error
                        | None -> ())
                    | _ -> ()
            }

    member __.CurrentEventsByTag (tag, offset) =
        Source.FromPublisher 
            {new IPublisher<EventEnvelope> with
                member __.Subscribe subscriber =
                    let result = 
                        operation {
                            let! search =
                                [Dsl.terms<PersistedEvent,string> <@ fun persistedEvent -> persistedEvent.Tags @> [tag]]
                                |> Dsl.bool [] [Dsl.range<PersistedEvent,int64> <@ fun persistedEvent -> persistedEvent.EventId @> (GreaterThanOrEqual offset) UnboundedUpper None] []
                                |> Dsl.execute<PersistedEvent> client None (Some <| Sort.ascending <@ fun persistedEvent -> persistedEvent.SequenceNumber @>) None None
                            search |> processHits subscriber
                        } |> Operation.wait
                    match result with
                    | Failure events -> events |> List.iter (fun event -> 
                        match event.ToException() with
                        | Some error -> subscriber.OnError error
                        | None -> ())
                    | _ -> ()
            }

    member __.CurrentPersistenceIds () =
        Source.FromPublisher
            {new IPublisher<string> with
                member __.Subscribe subscriber =
                    operation {
                        let! persistenceIds = getCurrentPersistenceIds () 
                        persistenceIds |> List.iter subscriber.OnNext
                    } |> Operation.returnOrFail
            }

    member __.AllPersistenceIds () =
        Source.FromPublisher 
            {new IPublisher<string> with
                member __.Subscribe subscriber =
                    operation {
                        let! currentIds = getCurrentPersistenceIds()
                        allPersistenceIdsActor <! RegisterSubscriber subscriber
                        currentIds |> List.iter (fun id -> subscriber.OnNext id)
                    } |> Operation.returnOrFail
            }

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

    interface IReadJournal
    interface IAllPersistenceIdsQuery with
        member journal.AllPersistenceIds () = 
            journal.AllPersistenceIds()           
    interface ICurrentEventsByPersistenceIdQuery with
        member journal.CurrentEventsByPersistenceId (persistenceId, fromSequence, toSequence) =
            journal.CurrentEventsByPersistenceId(persistenceId, fromSequence, toSequence)
    interface ICurrentEventsByTagQuery with
        member journal.CurrentEventsByTag (tag, offset) =
            journal.CurrentEventsByTag(tag, offset)
    interface ICurrentPersistenceIdsQuery with
        member journal.CurrentPersistenceIds () =
            journal.CurrentPersistenceIds()
    interface IEventsByPersistenceIdQuery with
        member journal.EventsByPersistenceId (persistenceId, fromSequence, toSequence) =
            journal.EventsByPersistenceId(persistenceId, fromSequence, toSequence)
    interface IEventsByTagQuery with
        member journal.EventsByTag (tag, offset) =  
            journal.EventsByTag(tag, offset)


type ElasticsearchReadJournalProvider (system: ExtendedActorSystem) =
    interface IReadJournalProvider with
        member __.GetReadJournal () = ElasticsearchReadJournal(system) :> IReadJournal

