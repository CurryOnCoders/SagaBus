namespace CurryOn.Akka

open Akka.Actor
open Akka.Configuration
open Akka.Persistence
open FSharp.Control
open System

type IEventJournal =
    abstract member GetCurrentPersistenceIds: unit -> Operation<Set<string>, PersistenceEvent>
    abstract member PersistEvents: JournaledEvent list -> Operation<unit, PersistenceEvent>
    abstract member DeleteEvents: string -> int64 -> Operation<unit, PersistenceEvent>
    abstract member GetMaxSequenceNumber: string -> int64 -> Operation<int64 option, PersistenceEvent>
    abstract member GetEvents: string -> int64 -> int64 -> int64 -> Operation<IPersistentRepresentation seq, PersistenceEvent>
    abstract member GetTaggedEvents: string -> int64 option -> int64 option -> Operation<TaggedEvent seq, PersistenceEvent>
    abstract member SaveSnapshot: JournalSnapshot -> Operation<unit, PersistenceEvent>
    abstract member GetSnapshot: string -> SnapshotSelectionCriteria -> Operation<JournalSnapshot option, PersistenceEvent>
    abstract member DeleteSnapshots: string -> SnapshotSelectionCriteria -> Operation<unit, PersistenceEvent>
    abstract member DeleteAllSnapshots: string -> int64 -> Operation<unit, PersistenceEvent>

type IEventJournalProvider =
    abstract member GetEventJournal: Config -> IActorContext -> IEventJournal