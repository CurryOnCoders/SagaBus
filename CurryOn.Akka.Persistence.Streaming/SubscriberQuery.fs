namespace CurryOn.Akka

open Akka
open Akka.Actor
open Akka.Configuration
open Akka.Streams.Dsl
open Akka.Persistence.Query
open CurryOn.Common
open FSharp.Control
open Reactive.Streams
open System
open System.Threading.Tasks

type IStreamingEventJournal =
    inherit IEventJournal
    abstract member SubscribeToPersistenceIds: unit -> IPublisher<string>
    abstract member SubscribeToEvents: string -> int64 -> IPublisher<JournaledEvent>

type IStreamingEventJournalProvider =
    inherit IEventJournalProvider
    abstract member GetStreamingEventJournal: Config -> ActorSystem -> IStreamingEventJournal

[<AbstractClass>]
type StreamingQueryReadJournalBase<'provider when 'provider :> IStreamingEventJournalProvider and 'provider: (new: unit -> 'provider)>(system: ExtendedActorSystem, config: Config, identifier: string) =
    let readBatchSize = config.GetInt("read-batch-size", 1024)
    let maxBufferSize = config.GetLong("max-buffer-size", 4096L)
    let provider = new 'provider() :> IStreamingEventJournalProvider
    let journal = provider.GetStreamingEventJournal config system

    member __.CurrentEventsByPersistenceId (persistenceId, fromSequence, toSequence) =
        Source.FromTask(journal.GetEvents persistenceId fromSequence toSequence Int64.MaxValue |> Operation.toTask)
              .SelectMany(fun events -> events)
              .Select(fun event -> EventEnvelope(0L, event.PersistenceId, event.SequenceNr, event.Payload))

    member __.CurrentEventsByTag (tag, offset) =
        Source.FromTask(journal.GetTaggedEvents tag (Some offset) None |> Operation.mapToTask (fun events-> events |> Seq.map (fun tagged -> tagged.Event)))
              .SelectMany(fun events -> events)
              .Select(fun event -> EventEnvelope(0L, event.PersistenceId, event.SequenceNr, event.Payload))

    member __.CurrentPersistenceIds () =
        Source.FromTask(journal.GetCurrentPersistenceIds() |> Operation.toTask)
              .SelectMany(fun id -> id :> System.Collections.Generic.IEnumerable<string>)

    member __.AllPersistenceIds () =
        Source.FromPublisher(journal.SubscribeToPersistenceIds ())

    member __.EventsByPersistenceId (persistenceId, fromSequence, toSequence) =
        Source.FromPublisher(journal.SubscribeToEvents persistenceId fromSequence)
              .TakeWhile(fun event -> event.SequenceNumber <= toSequence)
              .Select(fun event -> EventEnvelope(0L, event.PersistenceId, event.SequenceNumber, event.Event))

    member __.EventsByTag (tag, offset) =  
        Source.FromPublisher(journal.SubscribeToEvents "$all" offset)
              .Where(fun event -> event.Tags |> Seq.contains tag)
              .Select(fun event -> EventEnvelope(0L, event.PersistenceId, event.SequenceNumber, event.Event))            

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
