namespace Akka.Persistence.EventStore.Journal

open Akka.Actor
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

type EventStoreJournal (context: IActorContext) = 
    inherit AsyncWriteJournal()
    let plugin = EventStorePlugin(context)
    let config = lazy(context.System.Settings.Config.GetConfig("eventstore.persistence.journal"))
    let writeBatchSize = lazy(config.Value.GetInt("write-batch-size"))
    let readBatchSize = lazy(config.Value.GetInt("read-batch-size"))
    let eventStore = plugin.Connect () 

    override this.WriteMessagesAsync messages =
        task {
            let tasks = messages 
                        |> Seq.map (fun message -> (message, plugin.Serialization.Serialize message (message.Payload |> getTypeName |> Some)))
                        |> Seq.map (fun (message, event) ->
                            let expectedVersion =
                                let sequenceNumber = message.LowestSequenceNr - 1L
                                if sequenceNumber = 0L
                                then ExpectedVersion.NoStream |> int64
                                else sequenceNumber
                            eventStore.AppendToStreamAsync(message.PersistenceId, expectedVersion, plugin.Credentials, event))
                        |> Seq.toArray
            
            try 
                let! results = Task.WhenAll(tasks)
                return ImmutableList<exn>.Empty :> IImmutableList<exn>
            with | ex ->
                let errors = [ex]@(tasks |> Array.filter (fun task -> task.IsFaulted) |> Array.map (fun task -> task.Exception) |> Seq.cast<exn> |> Seq.toList)
                return ImmutableList.CreateRange(errors) :> IImmutableList<exn>
        }

    override this.DeleteMessagesToAsync (persistenceId, sequenceNumber) =
        task {
            let! metadataResult = eventStore.GetStreamMetadataAsync(persistenceId, plugin.Credentials)
            let metadata = metadataResult.StreamMetadata
            let newMetadata = StreamMetadata.Create(metadata.MaxCount, metadata.MaxAge, sequenceNumber |> Nullable, metadata.CacheControl, metadata.Acl)
            return! eventStore.SetStreamMetadataAsync(persistenceId, metadataResult.MetastreamVersion, newMetadata, plugin.Credentials)
        } :> Task

    override this.ReadHighestSequenceNrAsync (persistenceId, from) =
        task {
            let! eventResult = eventStore.ReadEventAsync(persistenceId, StreamPosition.End |> int64, true, plugin.Credentials)
            match eventResult.Status with
            | EventReadStatus.Success -> return eventResult.EventNumber
            | EventReadStatus.NotFound ->
                let! streamMetadata = eventStore.GetStreamMetadataAsync(persistenceId, plugin.Credentials) 
                return streamMetadata.StreamMetadata.TruncateBefore.GetValueOrDefault()
            | _ -> return 0L
        }
   
    override this.ReplayMessagesAsync (context, persistenceId, first, last, max, recoveryCallback) =
        task {
            let eventsToRead = Math.Min(last - first + 1L, max)
            let settings = CatchUpSubscriptionSettings(CatchUpSubscriptionSettings.Default.MaxLiveQueueSize, !readBatchSize, false, true)
            let messagesReplayed = ref 0L
            let toPersistentRepresentation (resolvedEvent: ResolvedEvent) =
                plugin.Serialization.Deserialize<Persistent> resolvedEvent.Event 
                :> IPersistentRepresentation
            let sendMessage (subscription: EventStoreCatchUpSubscription) (event: ResolvedEvent) =
                if event.OriginalEventNumber > last || Interlocked.Increment(messagesReplayed) > max
                then subscription.Stop()
                else event |> toPersistentRepresentation |> recoveryCallback.Invoke
            let liveProcessingStarted = new ManualResetEvent(false)
            let subscription = eventStore.SubscribeToStreamFrom(persistenceId, first |> Nullable, settings, 
                                                                (fun subscription event -> sendMessage subscription event), 
                                                                liveProcessingStarted = (fun _ -> liveProcessingStarted.Set() |> ignore), 
                                                                userCredentials = plugin.Credentials)
            return liveProcessingStarted.WaitOne()
        } :> Task
    