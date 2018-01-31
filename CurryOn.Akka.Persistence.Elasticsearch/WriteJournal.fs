namespace Akka.Persistence.Elasticsearch

open Akka
open Akka.Actor
open Akka.Configuration
open Akka.Event
open Akka.FSharp
open Akka.Persistence
open Akka.Persistence.Journal
open Akka.Streams
open Akka.Streams.Dsl
open CurryOn.Akka
open CurryOn.Common
open CurryOn.Elastic
open FSharp.Control
open System
open System.Collections.Concurrent
open System.Collections.Immutable
open System.Threading
open System.Threading.Tasks

module internal EventJournal =
    let searchForType = memoize <| Types.findType        

    let getEventType (persistedEvent: PersistedEvent) =
        searchForType persistedEvent.EventType |> Operation.returnOrFail

    let deserialize (serialization: ElasticsearchSerialization) (eventType: Type) (event: PersistedEvent) =
        let deserializer = serialization.GetType().GetMethod("Deserialize").MakeGenericMethod(eventType)
        deserializer.Invoke(serialization, [|event|])

type ElasticsearchJournal (config: Config) as journal = 
    inherit AsyncWriteJournal()
    let context = AsyncWriteJournal.Context
    let plugin = ElasticsearchPlugin(context)
    let writeBatchSize = config.GetInt("write-batch-size", 512)
    let readBatchSize = config.GetInt("read-batch-size", 1024)
    let persistenceIdSubscribers = new ConcurrentDictionary<string, Set<IActorRef>>()
    let tagSubscribers = new ConcurrentDictionary<string, Set<IActorRef>>()
    let client = plugin.Connect () 
    let mutable allPersistenceIdSubscribers = Set.empty<IActorRef>
    let mutable allPersistenceIds = 
        let result = client |> Search.distinct<PersistedEvent,string> <@ fun event -> event.PersistenceId @> None |> Operation.returnOrFail
        result.Results.Hits |> List.fold (fun acc hit -> acc |> Set.add hit.Document.PersistenceId) Set.empty<string> 
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

    let getEventsByTag = PersistenceQuery.getCurrentEventsByTag client
    let getPersistenceIds () = PersistenceQuery.getCurrentPersistenceIds client

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

    do match getPersistenceIds () |> Operation.wait with
       | Success success -> success.Result |> Seq.iter (fun persistenceId -> allPersistenceIds <- allPersistenceIds.Add persistenceId)
       | Failure errors -> raise <| OperationFailedException(errors)

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
                              EventType = persistentMessage.Payload |> getFullTypeName
                              Sender = persistentMessage.Sender
                              SequenceNumber = persistentMessage.SequenceNr
                              Event = persistentMessage.Payload |> Serialization.toJson
                              WriterId = persistentMessage.WriterGuid
                              Tags = tags }) |> Seq.toList

                        match events with
                        | [] -> 
                            return! Result.success List<DocumentId>.Empty
                        | [event] ->
                            let! result = client.Index({ Id = None; Document = event})
                            if maybeNewPersistenceId event.PersistenceId
                            then newPersistenceIds.Add(event.PersistenceId) |> ignore
                            return! Result.success [result.Id]
                        | events -> 
                            let! result = client.BulkIndex(events)
                            events |> Seq.iter (fun event -> 
                                if maybeNewPersistenceId event.PersistenceId
                                then newPersistenceIds.Add(event.PersistenceId) |> ignore)
                            return! result.Results |> List.map (fun r -> r.Id) |> Result.success
                    })          
                |> Operation.Parallel
        
            let! results = indexOperations |> Async.StartAsTask
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
            return! Query.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> Unbounded (Inclusive sequenceNumber)
                    |> Query.And (Query.field <@ fun (event: PersistedEvent) -> event.PersistenceId @> persistenceId)
                    |> Query.delete<PersistedEvent> client None None None None
                    |> SearchOperation.toTask
        } :> Task

    override this.ReadHighestSequenceNrAsync (persistenceId, from) =
        task {
            let! highestSequence = 
                Query.field<PersistedEvent, string> <@ fun event -> event.PersistenceId @> persistenceId
                |> Query.And (Query.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> (Inclusive from) Unbounded)
                |> Query.first<PersistedEvent> client None (Sort.descending <@ fun event -> event.SequenceNumber @>)
                |> Operation.waitTask
            return match highestSequence with
                   | Success success -> 
                        match success.Result with
                        | Some result -> result.SequenceNumber
                        | None -> 0L
                   | _ -> 0L
        }
   
    override this.ReplayMessagesAsync (context, persistenceId, first, last, max, recoveryCallback) =
        task {
            let! result =
                Query.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> (Inclusive first) (Inclusive last)
                |> Query.And (Query.field <@ fun (event: PersistedEvent) -> event.PersistenceId @> persistenceId)
                |> Query.execute<PersistedEvent> client None (Some <| Sort.ascending <@ fun event -> event.SequenceNumber @>) None (max |> int |> Some)
                |> SearchOperation.toTask
            return 
                result.Results.Hits 
                |> Seq.map (fun hit -> hit.Document :> IPersistentRepresentation)
                |> Seq.iter (fun persistent -> persistent |> recoveryCallback.Invoke)
        } :> Task

    override __.ReceivePluginInternal (message) =
        match message with
        | :? ReplayCommands as command ->
            match command with
            | ReplayTaggedMessages (fromOffset, toOffset, tag, actor) ->
                operation {
                    let lowOffset = if fromOffset = 0L then None else Some fromOffset
                    let highOffset = if toOffset = Int64.MaxValue then None else Some toOffset
                    let! taggedEvents = getEventsByTag tag lowOffset highOffset
                    taggedEvents.Results.Hits |> Seq.iter (fun hit -> actor <! ReplayedTaggedMessage(hit.Id.ToInt(), tag, hit.Document))
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

    
