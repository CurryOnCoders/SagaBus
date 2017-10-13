namespace Akka.Persistence.EventStore

open Akka.Persistence.Snapshot

type EventStoreSnapshotStore () =
    inherit SnapshotStore()

    member __.LoadAsync (persistenceId, criteria) =
        ()