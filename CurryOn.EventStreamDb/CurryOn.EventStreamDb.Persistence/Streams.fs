namespace CurryOn.EventStreamDb.Persistence

open Akka.Actor
open Akka.FSharp
open CurryOn.Common
open CurryOn.EventStreamDb
open System
open System.IO

type InMemoryStream (streamName) as stream =
    inherit ReceiveActor()
    let context = ActorBase.Context
    let eventStream = IndexedList<IEvent, Guid>(fun event -> event.Id)

    let appendEvent (write: WriteEvent<'a>) =
        () // TODO: Add event to eventStream

    do stream.Receive<WriteEvent<'a>>(fun (write: WriteEvent<'a>) -> 
        try appendEvent write
        with | ex -> context.Sender <! {Id = write.Id; Stream = write.Stream; Error = ex.ToString()})
