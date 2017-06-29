namespace Akka.Persistence.EventStore

open CurryOn.Common
open CurryOn.Core
open EventStore.ClientAPI
open EventStore.ClientAPI.Common
open EventStore.ClientAPI.SystemData
open FSharp.Control
open System
open System.Net.Http
open System.Threading.Tasks

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

    let store = 
        lazy {
            // TODO: Replace with code to read from HOCON to get EventStore connection information
            //let context = !Context.Current
            //let getEventStore = context.GetComponent<IConnectionManager<IEventStoreConnection>>()
            //return! match getEventStore with
            //        | Success eventStore -> 
            //            url <- eventStore.Url
            //            eventStore.ConnectAsync()
            //        | Failure ex -> raise <| Log.errorxr ex "Unable to Initialize EventStore Module (getEventStore)"
            return Unchecked.defaultof<IEventStoreConnection>
        }

    let credentials = lazy(
        // TODO: Replace with code to read from HOCON to get EventStore connection information
        //let context = !Context.Current
        //let getCredentials = context.GetComponent<ICredentialManager<UserCredentials>>()
        //match getCredentials with
        //| Success credentialManager -> credentialManager.GetCredentials({new IEndpoint with member __.Locator = ""})
        //| Failure ex -> raise <| Log.errorxr ex "Unable to Initialize EventStore Module (getCredentials)"
        ConnectionSettings.Default.DefaultUserCredentials
    )

    let rec private read stream condition from (getEvents: int64 -> Task<StreamEventsSlice>) =
        asyncSeq {
            let! eventSlice = getEvents from |> Async.AwaitTask
            yield eventSlice
            if eventSlice |> condition
            then yield! read stream condition eventSlice.NextEventNumber getEvents
        }

    let private readAllForward stream from =
        async {
            let! eventStore = store |> LazyAsync.value
            return!
                (fun start -> eventStore.ReadStreamEventsForwardAsync(stream, start, MaxEvents, true, !credentials)) 
                |> read stream (fun slice -> slice.IsEndOfStream |> not) from
                |> AsyncSeq.foldAsync mergeSlices EmptySlice
        }

    let private readAllBackward stream from =
        async {
            let! eventStore = store |> LazyAsync.value
            return!
                (fun start -> eventStore.ReadStreamEventsBackwardAsync(stream, start, MaxEvents, true, !credentials))
                |> read stream (fun slice -> slice.IsEndOfStream |> not) from
                |> AsyncSeq.foldAsync mergeSlices EmptySlice
        }

    let private readForward stream from limit =
        async {
            let! eventStore = store |> LazyAsync.value
            let terminus = from + limit
            return!
                (fun start -> eventStore.ReadStreamEventsForwardAsync(stream, start, Math.Min(MaxEvents, (terminus - start) |> int), true, !credentials)) 
                |> read stream (fun slice -> slice.IsEndOfStream |> not && slice.NextEventNumber <= terminus) from
                |> AsyncSeq.foldAsync mergeSlices EmptySlice
        }

    let private readBackward stream from limit =
        async {
            let! eventStore = store |> LazyAsync.value
            let terminus = from - limit
            return!
                (fun start -> eventStore.ReadStreamEventsBackwardAsync(stream, start, Math.Min(MaxEvents, (start - terminus) |> int), true, !credentials)) 
                |> read stream (fun slice -> slice.IsEndOfStream |> not && slice.NextEventNumber >= terminus) from
                |> AsyncSeq.foldAsync mergeSlices EmptySlice
        }

    let readStream stream = function
    | Forward -> readAllForward stream 0L
    | ForwardFrom start -> readAllForward stream start
    | ForwardFor (start,limit) -> readForward stream start limit
    | Backward -> readAllBackward stream -1L
    | BackwardFrom start -> readAllBackward stream start
    | BackwardFor (start,limit) -> readBackward stream start limit

    let readCategory category = readStream (sprintf "$ce-%s" category)

    let readEventType eventType = readStream (sprintf "$et-%s" eventType)

    let readEvent stream position = 
        async {
            let! eventStore = store |> LazyAsync.value
            return! eventStore.ReadEventAsync(stream, position, true, !credentials) |> Async.AwaitTask
        }

    let checkSubscriptions url stream group = async {
        try 
            use client = new HttpClient(BaseAddress = Uri url)
            let! json = client.GetStringAsync(sprintf "/subscriptions/%s" stream) |> Async.AwaitTask
            return json |> parseJson<Subscription[]> |> Array.filter (fun subscription -> subscription.GroupName = group)
        with | _ -> return [||]
    }

    let subscribe subscription eventHandler =
        async {
            let! eventStore = store |> LazyAsync.value
            match subscription with
            | Volatile stream ->
                let! subscription = eventStore.SubscribeToStreamAsync(stream, true, (fun _ event -> event |> eventHandler), userCredentials = !credentials) |> Async.AwaitTask
                return (fun () -> subscription.Unsubscribe())
            | CatchUp (stream,startingPosition) ->
                let defaultSettings = CatchUpSubscriptionSettings.Default
                let settings = CatchUpSubscriptionSettings(defaultSettings.MaxLiveQueueSize, defaultSettings.ReadBatchSize, false, true)
                let subscription = eventStore.SubscribeToStreamFrom(stream, startingPosition |> Nullable, settings, (fun _ event -> event |> eventHandler), userCredentials = !credentials)
                return (fun () -> subscription.Stop())
            | Persistent (stream,group,startingPosition,autoAcknowledge,strategy) ->
                let! subscriptions = checkSubscriptions url stream group
                if subscriptions |> Seq.isEmpty then
                    let settings = PersistentSubscriptionSettings.Create().ResolveLinkTos().StartFrom(startingPosition).WithNamedConsumerStrategy(strategy)
                    eventStore.CreatePersistentSubscriptionAsync(stream, group, settings.Build(), !credentials) |> ignore
                let! subscription = eventStore.ConnectToPersistentSubscriptionAsync(stream, group, (fun _ event ->  event |> eventHandler), userCredentials = !credentials, autoAck = autoAcknowledge) |> Async.AwaitTask
                return (fun () -> subscription.Stop(TimeSpan.MaxValue))
        }

    let private getEventData (event: IEventMessage) =
        let eventData =  event.Body |> toJsonBytes
        let metadata = {Id = event.Header.MessageId; 
                        CorrelationId = event.Header.CorrelationId;
                        AggregateName = event.Header.AggregateName; 
                        AggregateKey = event.Header.AggregateKey; 
                        DateTime = event.Header.DateSent; 
                        AssemblyQualifiedType = event |> getQualifiedTypeName;
                        Format = Json}                
        EventData(event.Header.MessageId, event |> getTypeName, true, eventData, metadata |> toJsonBytes)

    let appendEvent stream expectedVersion (event: IEventMessage) =
        async {
            let! eventStore = store |> LazyAsync.value
            return! eventStore.AppendToStreamAsync(stream, expectedVersion, getEventData event) |> Async.AwaitTask
        }

    let appendEvents stream expectedVersion (events: IEventMessage seq) =
        async {
            let! eventStore = store |> LazyAsync.value
            let eventData = events |> Seq.map getEventData |> Seq.toArray
            return! eventStore.AppendToStreamAsync(stream, expectedVersion, eventData) |> Async.AwaitTask
        }

    let appendEventWithMetadata stream expectedVersion (metadata: EventMetadata) (event: IEventMessage) =
        async {
            let! eventStore = store |> LazyAsync.value             
            //Log.debugf "Appending Event %s to Stream %s" (event |> getTypeName) stream
            let! result = eventStore.AppendToStreamAsync(stream, expectedVersion, EventData(event.Header.MessageId, event |> getTypeName, true, event.Body |> toJsonBytes, metadata |> toJsonBytes)) |> Async.AwaitTask
            //Log.debugf "Resulting Log Position: %A, Next Expected Version: %d" result.LogPosition result.NextExpectedVersion
            return ()
        }

    let getStreams request =
        async {
            let! streamFeed = Atom.readFeed "$streams" Forward
            return
                match request with
                | All -> streamFeed.Entries |> Array.Parallel.map (fun entry -> entry.Stream)
                | Category category ->
                    streamFeed.Entries
                    |> Array.filter (fun entry -> entry.Category = category)
                    |> Array.Parallel.map (fun entry -> entry.Stream)                    
        }

    let getKeys category =
        async {
            let! streams = getStreams <| Category category
            return streams |> Array.Parallel.map (fun stream ->
                let index = stream.IndexOf('-')
                if index >= 0
                then stream.Substring(index + 1)
                else stream
            ) |> Array.distinct
        }