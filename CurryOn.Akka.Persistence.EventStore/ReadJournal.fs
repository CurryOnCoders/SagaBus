namespace Akka.Persistence.EventStore

open Akka.Actor
open Akka.Persistence.Query
open CurryOn.Akka

type EventStoreReadJournal (system: ExtendedActorSystem) =        
    inherit QueryReadJournalBase(system, system.Settings.Config.GetConfig(EventStoreReadJournal.Identifier), EventStoreReadJournal.Identifier)
    static member Identifier = "akka.persistence.query.journal.event-store"

type EventStoreReadJournalProvider (system: ExtendedActorSystem) =
    interface IReadJournalProvider with
        member __.GetReadJournal () = EventStoreReadJournal(system) :> IReadJournal
        