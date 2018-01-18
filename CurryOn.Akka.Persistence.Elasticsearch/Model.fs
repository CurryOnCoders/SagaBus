namespace Akka.Persistence.Elasticsearch

open Akka.Actor
open Akka.Persistence
open CurryOn.Elastic
open Reactive.Streams
open System

module internal Formats =    
    [<Literal>]
    let Date = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"

type SerializedEvent = string
type SerializedSnapshot = string

[<CLIMutable>]
[<Indexed("persisted_event")>]
type PersistingEvent =
    {
        [<ExactMatch("persistence_id")>]  PersistenceId: string
        [<ExactMatch("event_type")>]      EventType: string
        [<NonIndexedObject("sender")>]    Sender: IActorRef
        [<Int64("sequence_number")>]      SequenceNumber: int64
        [<ExactMatch("writer_id")>]       WriterId: string
        [<FullText("event")>]             Event: SerializedEvent
        [<ExactMatch("tags")>]            Tags: string []
    }
    interface IPersistentRepresentation with
        member __.IsDeleted = false
        member this.Manifest = this.EventType
        member this.PersistenceId = this.PersistenceId
        member this.Sender = this.Sender
        member this.SequenceNr = this.SequenceNumber
        member this.WriterGuid = this.WriterId
        member this.Payload = this.Event |> Serialization.parseJson<obj>
        member this.WithPayload payload = 
            {this with Event = payload |> Serialization.toJson} :> IPersistentRepresentation
        member this.WithManifest manifest = 
            {this with EventType = manifest} :> IPersistentRepresentation
        member this.Update (sequenceNr, persistenceId, _, sender, writerGuid) = 
            {this with SequenceNumber = sequenceNr; PersistenceId = persistenceId; Sender = sender; WriterId = writerGuid} :> IPersistentRepresentation

[<CLIMutable>]
[<Indexed("persisted_event", IdProperty = "EventId")>]
type PersistedEvent =
    {
        [<Int64("id")>]                   EventId: int64
        [<ExactMatch("persistence_id")>]  PersistenceId: string
        [<ExactMatch("event_type")>]      EventType: string
        [<NonIndexedObject("sender")>]    Sender: IActorRef
        [<Int64("sequence_number")>]      SequenceNumber: int64
        [<ExactMatch("writer_id")>]       WriterId: string
        [<FullText("event")>]             Event: SerializedEvent
        [<ExactMatch("tags")>]            Tags: string []
    }
    interface IPersistentRepresentation with
        member __.IsDeleted = false
        member this.Manifest = this.EventType
        member this.PersistenceId = this.PersistenceId
        member this.Sender = this.Sender
        member this.SequenceNr = this.SequenceNumber
        member this.WriterGuid = this.WriterId
        member this.Payload = this.Event |> Serialization.parseJson<obj>
        member this.WithPayload payload = 
            {this with Event = payload |> Serialization.toJson} :> IPersistentRepresentation
        member this.WithManifest manifest = 
            {this with EventType = manifest} :> IPersistentRepresentation
        member this.Update (sequenceNr, persistenceId, _, sender, writerGuid) = 
            {this with SequenceNumber = sequenceNr; PersistenceId = persistenceId; Sender = sender; WriterId = writerGuid} :> IPersistentRepresentation

[<CLIMutable>]
[<Indexed("snapshot")>]
type Snapshot =
    {
        [<ExactMatch("persistence_id")>]    PersistenceId: string
        [<ExactMatch("snapshot_type")>]     SnapshotType: string
        [<Int64("sequence_number")>]        SequenceNumber: int64
        [<Date("timestamp", Formats.Date)>] Timestamp: DateTime
        [<FullText("state")>]               State: SerializedSnapshot
    }


type AllPersistenceIdsMessages =
    | RegisterSubscriber of ISubscriber<string>
    | NewPersistenceId of string