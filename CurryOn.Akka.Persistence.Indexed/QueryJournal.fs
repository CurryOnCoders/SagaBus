namespace CurryOn.Akka

open Akka
open Akka.Actor
open Akka.Configuration
open Akka.Streams.Dsl
open Akka.Persistence.Query

[<AbstractClass>]
type IndexedQueryReadJournalBase (system: ExtendedActorSystem, config: Config, identifier: string) =
    let readBatchSize = config.GetInt("read-batch-size", 1024)
    let maxBufferSize = config.GetLong("max-buffer-size", 4096L)

    member __.CurrentEventsByPersistenceId (persistenceId, fromSequence, toSequence) =
        Source.ActorPublisher<EventEnvelope>(EventsbyPersistenceIdPublisher.Props(false, persistenceId, fromSequence, toSequence, maxBufferSize, identifier))
              .MapMaterializedValue(fun _ -> NotUsed.Instance)
              .Named("EventsByPersistenceId") |> unbox<Source<EventEnvelope,NotUsed>>

    member __.CurrentEventsByTag (tag, offset) =
        Source.ActorPublisher<EventEnvelope>(EventsByTagPublisher.Props(false, tag, offset, maxBufferSize, identifier))
              .MapMaterializedValue(fun _ -> NotUsed.Instance)
              .Named("EventsByTag") |> unbox<Source<EventEnvelope,NotUsed>>

    member __.CurrentPersistenceIds () =
        Source.ActorPublisher<string>(PersistenceIdsPublisher.Props(false, identifier))
              .MapMaterializedValue(fun _ -> NotUsed.Instance)
              .Named("CurrentPesistenceIds") |> unbox<Source<string,NotUsed>>

    member __.AllPersistenceIds () =
        Source.ActorPublisher<string>(PersistenceIdsPublisher.Props(true, identifier))
              .MapMaterializedValue(fun _ -> NotUsed.Instance)
              .Named("AllPesistenceIds") |> unbox<Source<string,NotUsed>>

    member __.EventsByPersistenceId (persistenceId, fromSequence, toSequence) =
        Source.ActorPublisher<EventEnvelope>(EventsbyPersistenceIdPublisher.Props(true, persistenceId, fromSequence, toSequence, maxBufferSize, identifier))
              .MapMaterializedValue(fun _ -> NotUsed.Instance)
              .Named("EventsByPersistenceId") |> unbox<Source<EventEnvelope,NotUsed>>

    member __.EventsByTag (tag, offset) =  
        Source.ActorPublisher<EventEnvelope>(EventsByTagPublisher.Props(true, tag, offset, maxBufferSize, identifier))
              .MapMaterializedValue(fun _ -> NotUsed.Instance)
              .Named("EventsByTag") |> unbox<Source<EventEnvelope,NotUsed>>

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
