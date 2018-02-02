namespace CurryOn.Akka

open Akka.Actor
open Akka.Configuration
open Akka.Event
open Akka.FSharp
open Akka.Persistence
open Akka.Persistence.Journal
open CurryOn.Common
open FSharp.Control
open System
open System.Collections.Concurrent
open System.Collections.Immutable
open System.Threading
open System.Threading.Tasks

type IEventJournal =
    abstract member GetCurrentPersistenceIds: unit -> Operation<Set<string>, PersistenceEvent>
    abstract member PersistEvents: JournaledEvent list -> Operation<unit, PersistenceEvent>
    abstract member DeleteEvents: string -> int64 -> Operation<unit, PersistenceEvent>
    abstract member GetMaxSequenceNumber: string -> int64 -> Operation<int64 option, PersistenceEvent>
    abstract member GetEvents: string -> int64 -> int64 -> int64 -> Operation<IPersistentRepresentation seq, PersistenceEvent>
    abstract member GetTaggedEvents: string -> int64 option -> int64 option -> Operation<TaggedEvent seq, PersistenceEvent>
    abstract member SaveSnapshot: JournalSnapshot -> Operation<unit, PersistenceEvent>
    abstract member GetSnapshot: string -> SnapshotSelectionCriteria -> Operation<JournalSnapshot option, PersistenceEvent>
    abstract member DeleteSnapshots: string -> SnapshotSelectionCriteria -> Operation<unit, PersistenceEvent>
    abstract member DeleteAllSnapshots: string -> int64 -> Operation<unit, PersistenceEvent>

type IEventJournalProvider =
    abstract member GetEventJournal: Config -> IActorContext -> IEventJournal

[<AbstractClass>]
type StreamingEventJournal<'provider when 'provider :> IEventJournalProvider and 'provider: (new: unit -> 'provider)> (config: Config) as journal = 
    inherit AsyncWriteJournal()
    let context = AsyncWriteJournal.Context
    let persistenceIdSubscribers = new ConcurrentDictionary<string, Set<IActorRef>>()
    let tagSubscribers = new ConcurrentDictionary<string, Set<IActorRef>>()
    let provider = new 'provider() :> IEventJournalProvider
    let writeJournal = provider.GetEventJournal config context
    let mutable allPersistenceIdSubscribers = Set.empty<IActorRef>
    let mutable allPersistenceIds = writeJournal.GetCurrentPersistenceIds() |> Operation.returnOrFail
    let allPersistenceIdsLock = new ReaderWriterLockSlim()    
    let tagSequenceNr = ImmutableDictionary<string, int64>.Empty;
    
    let log = context.GetLogger()
    let handled _ = true
    let unhandled message = journal.UnhandledMessage message |> fun _ -> false

    let maybeNewPersistenceId persistenceId =
        let isNew = 
            try allPersistenceIdsLock.EnterUpgradeableReadLock()
                if allPersistenceIds.Contains persistenceId
                then false
                else try allPersistenceIdsLock.EnterWriteLock()
                         allPersistenceIds <- allPersistenceIds.Add persistenceId
                         true
                     finally allPersistenceIdsLock.ExitWriteLock()
            finally allPersistenceIdsLock.ExitUpgradeableReadLock()
        if isNew 
        then allPersistenceIdSubscribers |> Set.iter (fun subscriber -> subscriber <! PersistenceIdAdded(persistenceId))
        isNew

    let notifyPersistenceIdChange persistenceId =
        match persistenceIdSubscribers.TryGetValue(persistenceId) with
        | (true, subscribers) ->
            for subscriber in subscribers do subscriber <! EventAppended persistenceId
        | _ -> ()

    let notifyTagChange tag =
        match tagSubscribers.TryGetValue(tag) with
        | (true, subscribers) ->
            for subscriber in subscribers do subscriber <! TaggedEventAppended tag
        | _ -> ()

    static member Identifier = "akka.persistence.journal.elasticsearch"
    member this.UnhandledMessage message = base.Unhandled message

    override this.WriteMessagesAsync messages =
        task {
            let newPersistenceIds = Collections.Generic.HashSet<string>()
            let newTags = Collections.Generic.HashSet<string>()
            let indexOperations = 
                messages 
                |> Seq.map (fun message ->
                    operation {
                        let persistentMessages =  message.Payload |> unbox<IImmutableList<IPersistentRepresentation>> 
                        let events = persistentMessages |> Seq.map (fun persistentMessage ->
                            let eventType = persistentMessage.Payload |> getTypeName
                            let tags = 
                                match persistentMessage |> box with
                                | :? Tagged as tagged -> 
                                    let eventTags = tagged.Tags |> Seq.toArray
                                    eventTags |> Seq.iter (newTags.Add >> ignore)
                                    eventTags
                                | _ -> [||] 
                            { PersistenceId = persistentMessage.PersistenceId 
                              Manifest = persistentMessage.Payload |> getFullTypeName
                              Sender = persistentMessage.Sender
                              SequenceNumber = persistentMessage.SequenceNr
                              Event = persistentMessage.Payload
                              WriterId = persistentMessage.WriterGuid
                              Tags = tags }) |> Seq.toList

                        events |> List.iter (fun event -> 
                            if maybeNewPersistenceId event.PersistenceId
                            then newPersistenceIds.Add(event.PersistenceId) |> ignore)

                        return! writeJournal.PersistEvents events
                    })          
                |> Operation.Parallel
        
            let results = indexOperations |> Async.RunSynchronously
            let errors = results |> Array.fold (fun acc cur ->
                match cur with
                | Success _ -> acc
                | Failure events -> 
                    let exceptions = 
                        events 
                        |> List.map (fun event -> event.ToException()) 
                        |> List.filter (fun opt -> opt.IsSome) 
                        |> List.map (fun opt -> opt.Value)
                    acc @ exceptions) List<exn>.Empty

            if persistenceIdSubscribers.IsEmpty |> not
            then newPersistenceIds |> Seq.iter notifyPersistenceIdChange

            if tagSubscribers.IsEmpty |> not
            then newTags |> Seq.iter notifyTagChange

            return match errors with
                    | [] -> null
                    | _ -> ImmutableList.CreateRange(errors) :> IImmutableList<exn>
        } 

    override this.DeleteMessagesToAsync (persistenceId, sequenceNumber) =
        task {
            return! writeJournal.DeleteEvents persistenceId sequenceNumber
                    |> PersistenceOperation.toTask
        } :> Task

    override this.ReadHighestSequenceNrAsync (persistenceId, from) =
        task {
            let! highestSequence = writeJournal.GetMaxSequenceNumber persistenceId from |> Operation.waitTask
            return match highestSequence with
                   | Success success -> 
                        match success.Result with
                        | Some result -> result
                        | None -> 0L
                   | _ -> 0L
        }
   
    override this.ReplayMessagesAsync (context, persistenceId, first, last, max, recoveryCallback) =
        task {
            let! results = writeJournal.GetEvents persistenceId first last max |> PersistenceOperation.toTask
            return results |> Seq.iter (fun persistent -> persistent |> recoveryCallback.Invoke)
        } :> Task

    override __.ReceivePluginInternal (message) =
        match message with
        | :? ReplayCommands as command ->
            match command with
            | ReplayTaggedMessages (fromOffset, toOffset, tag, actor) ->
                operation {
                    let lowOffset = if fromOffset = 0L then None else Some fromOffset
                    let highOffset = if toOffset = Int64.MaxValue then None else Some toOffset
                    let! taggedEvents = writeJournal.GetTaggedEvents tag lowOffset highOffset
                    taggedEvents |> Seq.iter (fun hit -> actor <! ReplayedTaggedMessage(hit.Id, hit.Tag, hit.Event))
                    return! Result.success()
                } |> Operation.waitTask 
                  |> (fun task -> task.PipeTo(actor), ActorRefs.NoSender, fun h -> RecoverySuccess(h), fun e -> ReplayMessagesFailure(e))
                  |> handled
        | :? SubscriberCommands as command ->
            match command with
            | SubscribeToAllPersistenceIds ->
                allPersistenceIdSubscribers <- allPersistenceIdSubscribers.Add context.Sender
                context.Sender <! CurrentPersistenceIds(allPersistenceIds |> Set.toList)
                context.Watch(context.Sender) |> handled
            | SubscribeToPersistenceId persistenceId ->
                persistenceIdSubscribers.AddOrUpdate(persistenceId, Set.singleton context.Sender, fun _ set -> set.Add context.Sender) |> ignore
                context.Watch(context.Sender) |> handled
            | SubscribeToTag tag -> 
                tagSubscribers.AddOrUpdate(tag, Set.singleton context.Sender, fun _ set -> set.Add context.Sender) |> ignore
                context.Watch(context.Sender) |> handled
        | :? Terminated as terminated -> 
            persistenceIdSubscribers.Keys |> Seq.iter (fun key -> persistenceIdSubscribers.TryUpdate(key, persistenceIdSubscribers.[key] |> Set.remove terminated.ActorRef, persistenceIdSubscribers.[key]) |> ignore)
            tagSubscribers.Keys |> Seq.iter (fun key -> tagSubscribers.TryUpdate(key, tagSubscribers.[key] |> Set.remove terminated.ActorRef, tagSubscribers.[key]) |> ignore)
            allPersistenceIdSubscribers <- allPersistenceIdSubscribers |> Set.remove terminated.ActorRef
            handled ()
        | _ -> unhandled message

    


