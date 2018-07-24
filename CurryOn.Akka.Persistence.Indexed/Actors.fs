namespace CurryOn.Akka

open Akka
open Akka.Actor
open Akka.Event
open Akka.FSharp
open Akka.Persistence
open Akka.Persistence.Query
open Akka.Streams.Actors
open Akka.Streams.Dsl
open CurryOn.Akka
open CurryOn.Common
open FSharp.Control
open Reactive.Streams
open System
open System.Collections.Generic
open System.Collections.Concurrent

type DeliveryBuffer<'message>(deliver) =
    let buffer, bufferAll, deliver, isEmpty, length = 
        let mutable messages = Queue.empty<'message>
        (fun message -> 
            messages <- messages |> Queue.enqueue message),
        (fun messageSet ->
            messages <- messages |> Queue.enqueueAll messageSet),
        (fun demand -> 
            let loopBody () =
                match messages |> Queue.tryDequeue with
                | Some (message, queue) -> 
                    try message |> deliver
                    finally messages <- queue
                | None -> ()
            if demand = Int64.MaxValue
            then while messages.HasMessages do loopBody()
            else for _ in {1L..Math.Min(demand, messages.Length |> int64)} do loopBody()),
        (fun () -> messages.IsEmpty),
        (fun () -> messages.Length)
        
    member __.Buffer message = buffer message
    member __.BufferAll messageSet = bufferAll messageSet
    member __.Deliver demand = deliver demand
    member __.IsEmpty with get () = isEmpty()
    member __.Length with get () = length()
        
type PersistenceIdsPublisher (liveQuery, pluginId) as publisher =
    inherit ActorPublisher<string>()
    let context = ActorBase.Context
    let buffer = new DeliveryBuffer<string>(publisher.OnNext)
    let journal = Persistence.Instance.Apply(context.System).JournalFor(pluginId);
    let handled _= true
    let unhandled = publisher.UnhandledMessage >> (fun () -> false)

    member __.UnhandledMessage message = base.Unhandled message

    override __.Receive (message) =
        match message with
        | :? Request -> 
            journal <! SubscribeToAllPersistenceIds
            publisher.Become(fun message ->
                match message with
                | :? Request -> 
                    buffer.Deliver(publisher.TotalDemand)
                    if (not liveQuery) && buffer.IsEmpty
                    then publisher.OnCompleteThenStop()
                    handled ()
                | :? PersistenceIdSubscriptionCommands as command->  
                    match command with
                    | CurrentPersistenceIds current -> 
                        buffer.BufferAll(current)
                        buffer.Deliver(publisher.TotalDemand)
                        if (not liveQuery) && buffer.IsEmpty
                        then publisher.OnCompleteThenStop()
                        |> handled
                    | PersistenceIdAdded persistenceId -> 
                        if liveQuery 
                        then buffer.Buffer persistenceId
                             buffer.Deliver(publisher.TotalDemand)
                        handled ()
                | :? Cancel -> context.Stop(context.Self) |> handled
                | _ -> message |> unhandled)
            |> handled
        | :? Cancel -> context.Stop(context.Self) |> handled
        | _ -> message |> unhandled
              
    
    static member Props (liveQuery, pluginId) =
        Props.Create (<@ fun () -> new PersistenceIdsPublisher(liveQuery, pluginId) @> |> toProps<PersistenceIdsPublisher>)


type EventsbyPersistenceIdPublisher (liveQuery, persistenceId, fromSequence, toSequence, maxBufferSize: int64, pluginId) as publisher =
    inherit ActorPublisher<EventEnvelope>()
    let context = ActorBase.Context
    let log = context.GetLogger()
    let buffer = new DeliveryBuffer<EventEnvelope>(publisher.OnNext)
    let journal = Persistence.Instance.Apply(context.System).JournalFor(pluginId);
    let refreshInterval = TimeSpan.FromSeconds(5.0)
    let currentSequence = ref 0L
    let handled _= true
    let unhandled = publisher.UnhandledMessage >> (fun () -> false)

    let isTimeForReplay () = buffer.IsEmpty || (buffer.Length |> int64) <= (maxBufferSize / 2L) && !currentSequence <= !toSequence

    do if liveQuery
       then context.System.Scheduler.ScheduleTellRepeatedly(refreshInterval, refreshInterval, context.Self, Continue, context.Self);


    let receiveIdleRequest () =
        buffer.Deliver(publisher.TotalDemand)
        if buffer.IsEmpty && !currentSequence > !toSequence
        then publisher.OnCompleteThenStop()
        elif not liveQuery
        then context.Self <! Continue

    let rec receiveRecoverySuccess highestSequence =
        buffer.Deliver(publisher.TotalDemand)
        if liveQuery
        then if buffer.IsEmpty && !currentSequence > !toSequence
             then publisher.OnCompleteThenStop()
        else 
            if highestSequence < !toSequence
            then toSequence := highestSequence
            if buffer.IsEmpty && (!currentSequence > !toSequence || !currentSequence = fromSequence)
            then publisher.OnCompleteThenStop()
            else context.Self <! Continue
        context.Become(fun message ->
            match message with
            | :? EventSubscriptionCommands as command ->
                if isTimeForReplay()
                then replay()
                handled ()
            | :? Request -> receiveIdleRequest() |> handled
            | :? Cancel -> context.Stop(context.Self) |> handled
            | _ -> message |> unhandled)

    and replay () =
        let limit = maxBufferSize - (buffer.Length |> int64);
        log.Debug("request replay for persistenceId [{0}] from [{1}] to [{2}] limit [{3}]", persistenceId, !currentSequence, !toSequence, limit);
        journal <! new ReplayMessages(!currentSequence, !toSequence, limit, persistenceId, context.Self)
        context.Become(fun message -> 
            match message with
            | :? ReplayedMessage as replayed ->
                let sequence = replayed.Persistent.SequenceNr
                buffer.Buffer(new EventEnvelope(Offset.Sequence(sequence), persistenceId, sequence, replayed.Persistent.Payload))
                currentSequence := sequence + 1L
                buffer.Deliver(publisher.TotalDemand) |> handled
            | :? RecoverySuccess as success ->
                log.Debug("replay completed for persistenceId [{0}], currSeqNo [{1}]", persistenceId, !currentSequence)
                receiveRecoverySuccess success.HighestSequenceNr |> handled
            | :? ReplayMessagesFailure as failure ->
                log.Debug("replay failed for persistenceId [{0}], due to [{1}]", persistenceId, failure.Cause.Message)
                buffer.Deliver(publisher.TotalDemand)
                publisher.OnErrorThenStop(failure.Cause) |> handled
            | :? Request -> buffer.Deliver(publisher.TotalDemand) |> handled
            | :? EventSubscriptionCommands as command -> () |> handled // ignore during replay
            | :? Cancel -> context.Stop(context.Self) |> handled
            | _ -> message |> unhandled)

    override __.Receive (message) =
        match message with
        | :? Request -> 
            if liveQuery 
            then journal <! (SubscribeToPersistenceId persistenceId)
            replay () |> handled
        | :? EventSubscriptionCommands as command->  
            match command with
            | Continue -> handled ()
            | _ -> message |> unhandled
        | :? Cancel -> context.Stop(context.Self) |> handled
        | _ -> message |> unhandled

    member __.UnhandledMessage message = base.Unhandled message
    
    static member Props (liveQuery, persistenceId, fromSequence, toSequence, maxBufferSize, pluginId) =
        Props.Create (<@ fun () -> new EventsbyPersistenceIdPublisher(liveQuery, persistenceId, fromSequence, ref toSequence, maxBufferSize, pluginId) @> |> toProps<EventsbyPersistenceIdPublisher>)


type EventsByTagPublisher (liveQuery, tag, fromOffset, maxBufferSize: int64, pluginId) as publisher =
    inherit ActorPublisher<EventEnvelope>()
    let context = ActorBase.Context
    let log = context.GetLogger()
    let buffer = new DeliveryBuffer<EventEnvelope>(publisher.OnNext)
    let journal = Persistence.Instance.Apply(context.System).JournalFor(pluginId);
    let currentOffset = ref 0L
    let toOffset = ref 0L
    let handled _= true
    let unhandled = publisher.UnhandledMessage >> (fun () -> false)

    let isTimeForReplay () = buffer.IsEmpty || (buffer.Length |> int64) <= (maxBufferSize / 2L) && !currentOffset <= !toOffset

    let receiveIdleRequest () =
        buffer.Deliver(publisher.TotalDemand)
        if buffer.IsEmpty && !currentOffset > !toOffset
        then publisher.OnCompleteThenStop()
        elif not liveQuery
        then context.Self <! Continue

    let rec receiveRecoverySuccess highestOffset =
        buffer.Deliver(publisher.TotalDemand)
        if liveQuery
        then if buffer.IsEmpty && !currentOffset > !toOffset
             then publisher.OnCompleteThenStop()
        else 
            if highestOffset < !toOffset
            then toOffset := highestOffset
            if buffer.IsEmpty && (!currentOffset > !toOffset)
            then publisher.OnCompleteThenStop()
            else context.Self <! Continue
        context.Become(fun message ->
            match message with
            | :? EventSubscriptionCommands as command ->
                if isTimeForReplay()
                then replay()
                handled ()
            | :? Request -> receiveIdleRequest() |> handled
            | :? Cancel -> context.Stop(context.Self) |> handled
            | _ -> message |> unhandled)

    and replay () =
        let limit = maxBufferSize - (buffer.Length |> int64);
        log.Debug("request replay for tag [{0}] from [{1}] to [{2}] limit [{3}]", tag, !currentOffset, !toOffset, limit);
        journal <! ReplayTaggedMessages(!currentOffset, !toOffset, tag, context.Self)
        context.Become(fun message -> 
            match message with
            | :? ReplayEvents as event ->
                match event with
                | ReplayedTaggedMessage (eventOffset,eventTag,persistent) ->
                    buffer.Buffer(new EventEnvelope(Offset.Sequence(eventOffset), persistent.PersistenceId, persistent.SequenceNr, persistent.Payload))
                    currentOffset := eventOffset
                    buffer.Deliver(publisher.TotalDemand) |> handled
            | :? RecoverySuccess as success ->
                log.Debug("replay completed for tag [{0}], currSeqNo [{1}]", tag, !currentOffset)
                receiveRecoverySuccess success.HighestSequenceNr |> handled
            | :? ReplayMessagesFailure as failure ->
                log.Debug("replay failed for tag [{0}], due to [{1}]", tag, failure.Cause.Message)
                buffer.Deliver(publisher.TotalDemand)
                publisher.OnErrorThenStop(failure.Cause) |> handled
            | :? Request -> buffer.Deliver(publisher.TotalDemand) |> handled
            | :? EventSubscriptionCommands as command -> () |> handled // ignore during replay
            | :? Cancel -> context.Stop(context.Self) |> handled
            | _ -> message |> unhandled)

    let receiveInitialRequest () =
        if liveQuery
        then journal <! SubscribeToTag tag
        replay ()

    member __.UnhandledMessage message = base.Unhandled message

    override __.Receive (message) =
        match message with
        | :? Request -> receiveInitialRequest () |> handled
        | :? EventSubscriptionCommands as command->  
            match command with
            | Continue -> handled ()
            | _ -> message |> unhandled
        | :? Cancel -> context.Stop(context.Self) |> handled
        | _ -> message |> unhandled              
    
    static member Props (liveQuery, tag, fromOffset, maxBufferSize, pluginId) =
        Props.Create (<@ fun () -> new EventsByTagPublisher(liveQuery, tag, fromOffset, maxBufferSize, pluginId) @> |> toProps<EventsByTagPublisher>)