namespace CurryOn.Akka

open Akka.Actor
open Akka.Persistence
open FSharp.Control
open System
open System.Threading.Tasks

[<CLIMutable>]
type JournaledEvent =
    {
        PersistenceId: string
        Manifest: string
        Sender: IActorRef
        SequenceNumber: int64
        WriterId: string
        Event: obj
        Tags: string []
    }
    interface IPersistentRepresentation with
        member __.IsDeleted = false
        member this.Manifest = this.Manifest
        member this.PersistenceId = this.PersistenceId
        member this.Sender = this.Sender
        member this.SequenceNr = this.SequenceNumber
        member this.WriterGuid = this.WriterId
        member this.Payload = this.Event
        member this.WithPayload payload = 
            {this with Event = payload } :> IPersistentRepresentation
        member this.WithManifest manifest = 
            {this with Manifest = manifest} :> IPersistentRepresentation
        member this.Update (sequenceNr, persistenceId, _, sender, writerGuid) = 
            {this with SequenceNumber = sequenceNr; PersistenceId = persistenceId; Sender = sender; WriterId = writerGuid} :> IPersistentRepresentation

[<CLIMutable>]
type TaggedEvent =
    {
        Id: int64
        Tag: string
        Event: IPersistentRepresentation
    }

[<CLIMutable>]
type JournalSnapshot =
    {
        PersistenceId: string
        Manifest: string
        SequenceNumber: int64
        Timestamp: DateTime
        Snapshot: obj
    }


type PersistenceEvent =
    | PersistedSuccessfully
    | DeletedSuccessfully
    | PersistenceError of exn
    member this.ToException () =
        match this with
        | PersistenceError ex -> Some ex
        | _ -> None


module PersistenceOperation =
    exception PersistenceEventsException of PersistenceEvent []

    let inline private failed<'a> (result: OperationResult<'a,PersistenceEvent>) =
        result.Events |> List.toArray |> PersistenceEventsException

    let inline private failedTask<'a> result =
        result |> failed<'a> |> Task.FromException<'a>

    let inline private resultToTask<'a> result =
        match result with
        | Success success -> success.Result |> Task.FromResult<'a>
        | Failure _ -> result |> failedTask<'a>

    let inline toTask<'a> (operation: Operation<'a, PersistenceEvent>) =
        async {
            let! result = operation |> Operation.waitAsync
            return result |> resultToTask
        } |> Async.StartAsTask |> fun task -> task.Unwrap()