namespace CurryOn.Akka

open Akka.Actor
open System

[<CLIMutable>]
type EventMetadata =
    {
        EventType: string
        Sender: IActorRef
        Tags: string []
    }

[<CLIMutable>]
type SnapshotMetadata =
    {
        PersistenceId: string
        SequenceNumber: int64
        Timestamp: DateTime
    }

