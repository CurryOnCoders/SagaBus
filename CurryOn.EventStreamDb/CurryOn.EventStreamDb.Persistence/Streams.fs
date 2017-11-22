namespace CurryOn.EventStreamDb.Persistence

open Akka.Actor
open Akka.FSharp
open CurryOn.Common
open CurryOn.EventStreamDb
open System
open System.IO
open System.Threading
open System.Threading.Tasks

type InMemoryStream (streamName) as stream =
    inherit ReceiveActor()
    let context = ActorBase.Context
    let eventNumber = ref -1L
    let eventStream = IndexedList<IEvent, Guid>(fun event -> event.Id)

    let appendEvent (write: IWriteEvent) =
        async {
            let! streamPosition = eventStream.CountAsync()
            let eventType = write.EventData.GetType()
            let persistedEvent = 
                {
                    Id = write.Id
                    EventNumber = Interlocked.Increment(eventNumber)
                    StreamPosition = streamPosition |> int64
                    AbsolutePosition = streamPosition |> int64
                    StreamId = streamName
                    Timestamp = write.Timestamp
                    Metadata = {
                                 Type = {
                                          TypeName = eventType.Name
                                          ClrType = eventType.AssemblyQualifiedName
                                          Version = match write.EventVersion with
                                                    | Some version -> version
                                                    | None -> 0
                                        }
                                 Tags = write.Tags
                                 Serialization = JsonBinary
                                 UserData = [||] // TODO: Should user data be exposed in WriteEvent?
                               }
                    EventData = write.EventData |> Serialization.toJsonBytes
                }
            do! eventStream.AddAsync(persistedEvent)
            return persistedEvent.EventNumber
        }

    do stream.ReceiveAsync<IWriteEvent>(fun (write: IWriteEvent) -> 
        task {
            try let! eventNumber = appendEvent write |> Async.StartAsTask
                context.Sender <! {Id = write.Id; Stream = write.Stream; EventNumber = eventNumber}
            with | ex -> context.Sender <! {Id = write.Id; Stream = write.Stream; Error = ex.ToString()}
        } :> Task)
