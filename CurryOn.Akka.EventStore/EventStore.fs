namespace Akka.Persistence.EventStore

open Akka.FSharp
open CurryOn.Akka.EventStore
open CurryOn.Common
open CurryOn.Core
open EventStore.ClientAPI
open EventStore.ClientAPI.Common
open EventStore.ClientAPI.SystemData
open FSharp.Control
open System
open System.Net.Http
open System.Threading.Tasks
module Config = Akka.FSharp.Configuration

type GetStreamRequest =
| All
| Category of string

[<CLIMutable>]
type Link = {Href: string; Rel: string}

[<CLIMutable>]
type Subscription = 
    { Links: Link [];
      EventStreamId: string;
      GroupName: string;
      ParkedMessageUri: string;
      GetMessagesUri: string;
      Status: string;
      AverageItemsPerSecond: float;
      TotalItemsProcessed: int64;
      LastProcessedEventNumber: int64;
      LastKnownEventNumber: int64;
      ConnectionCount: int;
      TotalInFlightMessages: int; }

[<CLIMutable>]
type EventMetadata = 
    { Id: Guid; 
      CorrelationId: Guid;
      AggregateName: string; 
      AggregateKey: string; 
      DateTime: DateTime; 
      AssemblyQualifiedType: string;
      Format: SerializationFormat }

[<CLIMutable>]
type EventStreams =
    {
        Position: int
        Streams: string list
    }
    static member Empty = {Position = 0; Streams = List.empty}
    member this.Categories = 
        this.Streams |> List.groupBy (fun stream -> if stream.Length > 0 && stream.IndexOf('-') > 0
                                                    then stream.Substring(0, stream.IndexOf('-'))
                                                    else stream) |> Map.ofList

type IEventSlice =
    abstract member Stream: string
    abstract member Status: SliceReadStatus
    abstract member FromEventNumber: int64
    abstract member NextEventNumber: int64
    abstract member LastEventNumber: int64
    abstract member IsEndOfStream: bool
    abstract member ReadDirection: ReadDirection
    abstract member Events: ResolvedEvent[]


type EventSubscription =
| Volatile of string
| CatchUp of (string*int64)
| Persistent of (string*string*int64*bool*string)


module EventStore =
    [<Literal>]
    let MaxEvents = 4095
    let EmptySlice = {new IEventSlice with
                            member __.Stream = String.Empty
                            member __.Status = SliceReadStatus.StreamNotFound
                            member __.FromEventNumber = 0L
                            member __.NextEventNumber = 0L
                            member __.LastEventNumber = 0L
                            member __.ReadDirection = ReadDirection.Forward
                            member __.IsEndOfStream = true
                            member __.Events = [||]}
    
    let mutable private url = String.Empty

    let private toSlice : StreamEventsSlice -> IEventSlice = fun slice ->
        {new IEventSlice with
             member __.Stream = slice.Stream
             member __.Status = slice.Status
             member __.FromEventNumber = slice.FromEventNumber
             member __.NextEventNumber = slice.NextEventNumber
             member __.LastEventNumber = slice.LastEventNumber
             member __.ReadDirection = slice.ReadDirection
             member __.IsEndOfStream = slice.IsEndOfStream
             member __.Events = slice.Events}

    let private mergeSlices : IEventSlice -> StreamEventsSlice -> Async<IEventSlice> = fun acc slice -> 
        async { 
            return {new IEventSlice with
                member __.Stream = slice.Stream
                member __.Status = if acc.Status = SliceReadStatus.Success then slice.Status else acc.Status
                member __.FromEventNumber = Math.Min(acc.FromEventNumber, slice.FromEventNumber)
                member __.NextEventNumber = Math.Max(acc.NextEventNumber, slice.NextEventNumber)
                member __.LastEventNumber = Math.Max(acc.LastEventNumber, slice.LastEventNumber)
                member __.ReadDirection = slice.ReadDirection
                member __.IsEndOfStream = acc.IsEndOfStream || slice.IsEndOfStream
                member __.Events = Array.concat [|acc.Events; slice.Events|]
            } 
        }

    let settings = lazy(Settings.Load <| Config.load().GetConfig("akka.persistence.journal.event-store"))

    let store = 
        defer {
            let! connectionSettings = settings
            return EventStoreConnection.create connectionSettings
        }

    let credentials = 
        defer  {
            let! connectionSettings = settings
            return UserCredentials(connectionSettings.UserName, connectionSettings.Password)
        }
