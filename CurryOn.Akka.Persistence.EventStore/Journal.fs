namespace Akka.Persistence.EventStore

open Akka.Actor
open Akka.Configuration
open Akka.Persistence
open Akka.Persistence.EventStore
open Akka.Persistence.Journal
open Akka.Streams
open Akka.Streams.Dsl
open CurryOn.Common
open EventStore.ClientAPI
open System
open System.Collections.Immutable
open System.Threading
open System.Threading.Tasks

type EventStoreJournal (config: Config) = 
    inherit AsyncWriteJournal()
    let plugin = EventStorePlugin(AsyncWriteJournal.Context)
    let writeBatchSize = lazy(config.GetInt("write-batch-size"))
    let readBatchSize = lazy(config.GetInt("read-batch-size"))
    let connect () = plugin.Connect () 

    override this.WriteMessagesAsync messages =
        task {
            let! eventStore = connect()
            let tasks = messages 
                        |> Seq.map (fun message ->
                            let persistentMessages =  message.Payload |> unbox<IImmutableList<IPersistentRepresentation>> 
                            let events = persistentMessages |> Seq.map (fun persistentMessage ->
                                let eventType = persistentMessage.Payload |> getTypeName
                                let tags = 
                                    match persistentMessage |> box with
                                    | :? Tagged as tagged -> tagged.Tags |> Seq.toArray
                                    | _ -> [||] 
                                let eventMetadata = {EventType = persistentMessage.Payload |> getFullTypeName; Sender = persistentMessage.Sender; Size = message.Size; Tags = tags}
                                plugin.Serialization.Serialize persistentMessage.Payload (Some eventType) eventMetadata)
                            let expectedVersion =
                                let sequenceNumber = message.LowestSequenceNr - 1L
                                if sequenceNumber = 0L
                                then ExpectedVersion.NoStream |> int64
                                else sequenceNumber - 1L
                            eventStore.AppendToStreamAsync(message.PersistenceId, expectedVersion, plugin.Credentials, events |> Seq.toArray))
                        |> Seq.toArray            
            try 
                let! results = Task.WhenAll(tasks)
                return null
            with | ex ->
                let errors = [ex]@(tasks |> Array.filter (fun task -> task.IsFaulted) |> Array.map (fun task -> task.Exception) |> Seq.cast<exn> |> Seq.toList)
                return ImmutableList.CreateRange(errors) :> IImmutableList<exn>
        }

    override this.DeleteMessagesToAsync (persistenceId, sequenceNumber) =
        task {
            let! eventStore = connect()
            let! metadataResult = eventStore.GetStreamMetadataAsync(persistenceId, plugin.Credentials)
            let metadata = metadataResult.StreamMetadata
            let newMetadata = StreamMetadata.Create(metadata.MaxCount, metadata.MaxAge, sequenceNumber |> Nullable, metadata.CacheControl, metadata.Acl)
            return! eventStore.SetStreamMetadataAsync(persistenceId, metadataResult.MetastreamVersion, newMetadata, plugin.Credentials)
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
            | EventReadStatus.NotFound ->
                let! streamMetadata = eventStore.GetStreamMetadataAsync(persistenceId, plugin.Credentials) 
                return streamMetadata.StreamMetadata.TruncateBefore.GetValueOrDefault()
            | _ -> return 0L
        }
   
    override this.ReplayMessagesAsync (context, persistenceId, first, last, max, recoveryCallback) =
        task {
            let! eventStore = connect()
            let start = first - 1L
            let eventsToRead = Math.Min(last - start + 1L, max)
            let settings = CatchUpSubscriptionSettings(CatchUpSubscriptionSettings.Default.MaxLiveQueueSize, !readBatchSize, false, true)
            let messagesReplayed = ref 0L
            let toPersistentRepresentation (resolvedEvent: ResolvedEvent) =
                let deserializedObject = plugin.Serialization.Deserialize<obj> resolvedEvent.Event 
                let metadata = resolvedEvent.Event.Metadata |> Serialization.parseJsonBytes<EventMetadata>
                let persistent = Akka.Persistence.Persistent(deserializedObject, resolvedEvent.Event.EventNumber, resolvedEvent.Event.EventStreamId, metadata.EventType, false, metadata.Sender)
                persistent :> IPersistentRepresentation
            let sendMessage (subscription: EventStoreCatchUpSubscription) (event: ResolvedEvent) =
                if event.OriginalEventNumber > last || Interlocked.Increment(messagesReplayed) > max
                then subscription.Stop()
                else event |> toPersistentRepresentation |> recoveryCallback.Invoke
            let subscription = eventStore.SubscribeToStreamFrom(persistenceId, start |> Nullable, settings, 
                                                                (fun subscription event -> sendMessage subscription event), 
                                                                userCredentials = plugin.Credentials)
            return ()
        } :> Task
    