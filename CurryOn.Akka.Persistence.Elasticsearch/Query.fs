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

    let hitToEventEnvelope (hit: Hit<PersistedEvent>) =
        EventEnvelope(hit.Id |> toOffset, hit.Document.PersistenceId, hit.Document.SequenceNumber, hit.Document.Event |> Serialization.parseJson<obj>)

    let processHits (subscriber: ISubscriber<EventEnvelope>) (search: SearchResult<PersistedEvent>)  =
        search.Results.Hits
        |> List.map hitToEventEnvelope
        |> List.iter subscriber.OnNext

    let getCurrentPersistenceIds () =
        operation {
            let! distinctValues = client |> Search.distinct<PersistedEvent,string> <@ fun persistedEvent -> persistedEvent.PersistenceId @> None
            return! distinctValues.Aggregations |> List.map (fun value -> value.Key) |> Result.success
        }

    let getCurrentEvents () =
        operation {
            let sort = Some <| Sort.ascending <@ fun (persistedEvent: PersistedEvent) -> persistedEvent.SequenceNumber @>
            return! MatchAll None |> Dsl.execute<PersistedEvent> client None sort None None
        } 

    let getCurrentEventsByTag tag offset =
        operation {
            return! [Dsl.terms<PersistedEvent,string> <@ fun persistedEvent -> persistedEvent.Tags @> [tag]]
                    |> Dsl.bool [] [Dsl.range<PersistedEvent,int64> <@ fun persistedEvent -> persistedEvent.EventId @> (GreaterThanOrEqual offset) UnboundedUpper None] []
                    |> Dsl.execute<PersistedEvent> client None (Some <| Sort.ascending <@ fun persistedEvent -> persistedEvent.SequenceNumber @>) None None
        }

    let getCurrentEventsByPersistenceId persistenceId min max =
        operation {
            let inline toBound opt = 
                match opt with
                | Some value -> Inclusive value
                | None -> Unbounded
            let fromSequence = min |> toBound
            let toSequence = max |> toBound
            return!
                Query.field<PersistedEvent,string> <@ fun persistedEvent -> persistedEvent.PersistenceId @> persistenceId
                |> Query.And (Query.range<PersistedEvent,int64> <@ fun persistedEvent -> persistedEvent.SequenceNumber @> fromSequence toSequence)
                |> Query.execute<PersistedEvent> client None (Some <| Sort.ascending <@ fun persistedEvent -> persistedEvent.SequenceNumber @>) None None
        } 

    let allPersistenceIdsActor = 
        let actor = 
            spawn system "all-persistence-ids"
            <| fun (inbox: AllPersistenceIdsMessages Actor) ->
                let subscribers = new List<ISubscriber<string>>()
                let knownPersistenceIds = new HashSet<string>()
                let rec messageLoop () =
                    actor {
                        let! message = inbox.Receive()
                        match message with
                        | RegisterSubscriber subscriber -> 
                            subscribers.Add(subscriber)
                            knownPersistenceIds |> Seq.iter subscriber.OnNext
                        | NewPersistenceId persistenceId -> 
                            if knownPersistenceIds.Add(persistenceId)
                            then subscribers |> Seq.iter (fun subscriber -> subscriber.OnNext persistenceId)
                        return! messageLoop ()
                    }
                messageLoop ()
        operation {
            let! currentIds = getCurrentPersistenceIds()
            for id in currentIds
                do actor <! NewPersistenceId id
            return! Result.success actor
        } |> Operation.returnOrFail        

    let eventsByPersistenceIdActor = 
        let actor = 
            spawn system "events-by-persistence-id"
            <| fun (inbox: EventsByPersistenceIdMessages Actor) ->
                let subscribers = new Dictionary<string, List<ISubscriber<EventEnvelope>>>()
                let knownEvents = new HashSet<EventEnvelope>()
                let rec messageLoop () =
                    actor {
                        let! message = inbox.Receive()
                        match message with
                        | RegisterEventSubscriber register -> 
                            if subscribers.ContainsKey(register.PersistenceId) |> not
                            then subscribers.Add(register.PersistenceId, new List<ISubscriber<EventEnvelope>>())
                            subscribers.[register.PersistenceId].Add(register.Subscriber)
                            knownEvents 
                            |> Seq.filter (fun event -> event.PersistenceId = register.PersistenceId && event.SequenceNr >= register.FromSequence && event.SequenceNr <= register.ToSequence) 
                            |> Seq.iter register.Subscriber.OnNext
                        | NewEvent event ->                             
                            if knownEvents.Add(event)
                            then subscribers.[event.PersistenceId] |> Seq.iter (fun subscriber -> subscriber.OnNext event)
                        return! messageLoop ()
                    }
                messageLoop ()
        operation {
            let! currentEvents = getCurrentEvents ()
            for hit in currentEvents.Results.Hits
                do actor <! NewEvent (hit |> hitToEventEnvelope)
            return! Result.success actor
        } |> Operation.returnOrFail

    let eventsByTagActor = 
        let actor = 
            spawn system "events-by-tag"
            <| fun (inbox: EventsByTagMessages Actor) ->
                let subscribers = new Dictionary<string, List<ISubscriber<EventEnvelope>>>()
                let knownEvents = new HashSet<TaggedEvent>()
                let rec messageLoop () =
                    actor {
                        let! message = inbox.Receive()
                        match message with
                        | RegisterTagSubscriber register -> 
                            if subscribers.ContainsKey(register.Tag) |> not
                            then subscribers.Add(register.Tag, new List<ISubscriber<EventEnvelope>>())
                            subscribers.[register.Tag].Add(register.Subscriber)
                            knownEvents 
                            |> Seq.filter (fun event -> event.Tag = register.Tag && event.Event.Offset <= register.Offset) 
                            |> Seq.iter (fun tagged -> tagged.Event |> register.Subscriber.OnNext)
                        | NewTaggedEvent taggedEvent ->                             
                            if knownEvents.Add(taggedEvent)
                            then subscribers.[taggedEvent.Tag] |> Seq.iter (fun subscriber -> subscriber.OnNext taggedEvent.Event)
                        return! messageLoop ()
                    }
                messageLoop ()
        operation {
            let! currentEvents = getCurrentEvents ()
            for hit in currentEvents.Results.Hits do
                for tag in hit.Document.Tags do
                    do actor <! NewTaggedEvent {Tag = tag; Event = hit |> hitToEventEnvelope}
            return! Result.success actor
        } |> Operation.returnOrFail

    static member Identifier = "elasticsearch.persistence.query"

    member __.CurrentEventsByPersistenceId (persistenceId, fromSequence, toSequence) =
        Source.FromPublisher
            {new IPublisher<EventEnvelope> with
                member __.Subscribe subscriber =
                    let result = 
                        operation {
                            let! search = getCurrentEventsByPersistenceId persistenceId (Some fromSequence) (Some toSequence)
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
                            let! search = getCurrentEventsByTag tag offset                                
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
                    allPersistenceIdsActor <! RegisterSubscriber subscriber
            }

    member __.EventsByPersistenceId (persistenceId, fromSequence, toSequence) =
        Source.FromPublisher
            {new IPublisher<EventEnvelope> with
                member __.Subscribe subscriber =
                    let registration = {Subscriber = subscriber; PersistenceId = persistenceId; FromSequence = fromSequence; ToSequence = if toSequence > 0L then toSequence else Int64.MaxValue}
                    eventsByPersistenceIdActor <! RegisterEventSubscriber registration
            }

    member __.EventsByTag (tag, offset) =  
        Source.FromPublisher
            {new IPublisher<EventEnvelope> with
                member __.Subscribe subscriber =
                    let registration = {Subscriber = subscriber; Tag = tag; Offset = offset}
                    eventsByTagActor <! RegisterTagSubscriber registration
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

