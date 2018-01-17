namespace Akka.Persistence.Elasticsearch

open Akka.Actor
open Akka.Configuration
open Akka.Persistence
open Akka.Persistence.Journal
open Akka.Streams
open Akka.Streams.Dsl
open CurryOn.Common
open CurryOn.Elastic
open FSharp.Control
open System
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

type ElasticsearchJournal (config: Config) = 
    inherit AsyncWriteJournal()
    let context = AsyncWriteJournal.Context
    let plugin = ElasticsearchPlugin(context)
    let writeBatchSize = lazy(config.GetInt("write-batch-size"))
    let readBatchSize = lazy(config.GetInt("read-batch-size"))
    let connect () = plugin.Connect () 

    override this.WriteMessagesAsync messages =
        task {
            let client = connect()
            let indexOperations = 
                messages 
                |> Seq.map (fun message ->
                    operation {
                        let persistentMessages =  message.Payload |> unbox<IImmutableList<IPersistentRepresentation>> 
                        let events = persistentMessages |> Seq.map (fun persistentMessage ->
                            let eventType = persistentMessage.Payload |> getTypeName
                            let tags = 
                                match persistentMessage |> box with
                                | :? Tagged as tagged -> tagged.Tags |> Seq.toArray
                                | _ -> [||] 
                            { PersistenceId = persistentMessage.PersistenceId 
                              EventType = persistentMessage.Payload |> getFullTypeName
                              Sender = persistentMessage.Sender
                              SequenceNumber = persistentMessage.SequenceNr
                              Event = persistentMessage.Payload |> Serialization.toJson
                              WriterId = persistentMessage.WriterGuid
                              Tags = tags}) |> Seq.toList
                        match events with
                        | [] -> 
                            return! Result.success List<DocumentId>.Empty
                        | [event] ->
                            let! result = client.Index({ Id = None; Document = event})
                            return! Result.success [result.Id]
                        | events -> 
                            let! result = client.BulkIndex(events)
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
            return ImmutableList.CreateRange(errors) :> IImmutableList<exn>
        }

    override this.DeleteMessagesToAsync (persistenceId, sequenceNumber) =
        task {
            let client = connect()
            let query = Query.field <@ fun (event: PersistedEvent) -> event.PersistenceId @> persistenceId
                        |> Query.combine And (Query.range <@ fun (event: PersistedEvent) -> event.SequenceNumber @> Unbounded (Inclusive sequenceNumber))
                        |> Query.build
                        |> Search.build None None None None
            let! result = client.DeleteByQuery(query) |> Operation.waitTask
        } :> Task

    override this.ReadHighestSequenceNrAsync (persistenceId, from) =
        task {
            let! eventStore = connect()
            let! eventResult = eventStore.ReadEventAsync(persistenceId, StreamPosition.End |> int64, true, plugin.Credentials)
            match eventResult.Status with
            | EventReadStatus.Success -> return if eventResult.Event.HasValue
                                                then if eventResult.Event.Value.Event |> isNotNull
                                                     then eventResult.Event.Value.Event.EventNumber
                                                     else eventResult.Event.Value.OriginalEventNumber
                                                else eventResult.EventNumber
                                                + 1L
            | EventReadStatus.NotFound ->
                let! streamMetadata = eventStore.GetStreamMetadataAsync(persistenceId, plugin.Credentials) 
                return streamMetadata.StreamMetadata.TruncateBefore.GetValueOrDefault()
            | _ -> return 0L
        }
   
    override this.ReplayMessagesAsync (context, persistenceId, first, last, max, recoveryCallback) =
        task {
            let! eventStore = connect()
            let stopped = AsyncManualResetEvent(initialState = false)
            let start = Math.Max(0L, first - 2L)
            let eventsToRead = Math.Min(last - start + 1L, max)
            let settings = CatchUpSubscriptionSettings(CatchUpSubscriptionSettings.Default.MaxLiveQueueSize, !readBatchSize, false, true)
            let messagesReplayed = ref 0L
            let stop (subscription: EventStoreCatchUpSubscription) =
                subscription.Stop()
                stopped.Set()
            let toPersistentRepresentation (resolvedEvent: ResolvedEvent) =
                let deserializedObject = plugin.Serialization.Deserialize<obj> resolvedEvent.Event 
                let metadata = resolvedEvent.Event.Metadata |> Serialization.parseJsonBytes<EventMetadata>
                let persistent = Akka.Persistence.Persistent(deserializedObject, resolvedEvent.Event.EventNumber + 1L, resolvedEvent.Event.EventStreamId, metadata.EventType, false, metadata.Sender)
                persistent :> IPersistentRepresentation
            let sendMessage subscription (event: ResolvedEvent) =
                try let persistentEvent = event |> toPersistentRepresentation
                    persistentEvent |> recoveryCallback.Invoke
                with | ex -> context.System.Log.Error(ex, sprintf "Error Applying Recovered %s Event from EventStore for %s" event.Event.EventType persistenceId)
                if event.OriginalEventNumber + 1L >= last || Interlocked.Increment(messagesReplayed) > max
                then stop subscription
            let subscription = eventStore.SubscribeToStreamFrom(persistenceId, start |> Nullable, settings, 
                                                                (fun subscription event -> sendMessage subscription event), 
                                                                userCredentials = plugin.Credentials)
            return! stopped.WaitAsync() |> Task.ofUnit
        } :> Task
    