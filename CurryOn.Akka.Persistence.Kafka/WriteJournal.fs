namespace Akka.Persistence.Kafka

open Akka
open Akka.Actor
open Akka.Configuration
open Akka.Persistence
open CurryOn.Akka
open CurryOn.Common
open FSharp.Control
open Kafunk
open System

module EventJournal =
    let get (config: Config) (system: ActorSystem) =
        let plugin = KafkaPlugin(system)
        let kafka = plugin.Connect() |> Operation.returnOrFail
        let readBufferSize = config.GetInt("read-buffer-size", 65535)
        let getProducerConfig = 
            // We can only have one partition per topic in the Event Journal streams, because we need to preserve message ordering.
            // An alternative solution would be to use the persistence id as the key rather than the topic, but there's no way
            // to retrieve a list of all unique keys from Kafka, so we couldn't easily implement the CurrentPersistenceIds or
            // AllPersistenceIds queries required by the Read Journal.
            memoize <| fun topic -> ProducerConfig.create (topic, Partitioner.konst 0, RequiredAcks.Local)
        let getProducer = 
            memoize <| fun topic ->
                operation {
                    let producerConfig = getProducerConfig topic
                    let! producer = Producer.createAsync kafka producerConfig
                    return producer
                }
        let getConsumerConfig =
            memoize <| fun topic -> ConsumerConfig.create (plugin.ConsumerGroup, topic, readBufferSize)
        let getConsumer =
            memoize <| fun topic ->
                operation {
                    let consumerConfig = getConsumerConfig topic
                    let! consumer = Consumer.createAsync kafka consumerConfig
                    return consumer
                }
        let serializeEvent (event: JournaledEvent) =
            ProducerMessage.ofBytes (event |> Serialization.toJsonBytes)
        let deserializeEvent (value: byte ArraySegment) =
            value.Array |> Serialization.parseJsonBytes<obj> |> unbox<JournaledEvent>
        let publishEvents persistenceId events =
            operation {
                let! producer = getProducer persistenceId
                let producerEvents = events |> Seq.map serializeEvent
                let! results = Producer.produceBatched producer producerEvents
                return results
            }
        do () // todo: remove this when the code below is working.
        //let serializeSnapshotMetadata (snapshot: JournalSnapshot) =
        //    let metadata = {PersistenceId = snapshot.PersistenceId; SequenceNumber = snapshot.SequenceNumber; Timestamp = snapshot.Timestamp}
        //    metadata |> toJsonBytes
        //let deserializeSnapshotMetadata (resolvedEvent: ResolvedEvent) = 
        //    resolvedEvent.Event.Metadata |> parseJsonBytes<SnapshotMetadata>
        //let serializeEvent (event: JournaledEvent) = 
        //    let eventMetadata = {SequenceNumber = event.SequenceNumber; EventType = event.Manifest; Sender = event.Sender; Tags = event.Tags}
        //    EventData(Guid.NewGuid(), event.Manifest, true, event.Event |> box |> toJsonBytes, eventMetadata |> toJsonBytes)
        //let deserializeEvent (resolvedEvent: ResolvedEvent) =
        //    let metadata = resolvedEvent.Event.Metadata |> parseJsonBytes<EventMetadata>
        //    let event = resolvedEvent.Event.Data |> parseJsonBytes<obj>
        //    (event, metadata)
        //let getJournaledEvent resolvedEvent =
        //    let event, metadata = deserializeEvent resolvedEvent
        //    { PersistenceId = resolvedEvent.Event.EventStreamId
        //      SequenceNumber = metadata.SequenceNumber
        //      Sender = metadata.Sender
        //      Manifest = metadata.EventType 
        //      WriterId = Guid.NewGuid() |> string
        //      Event = event
        //      Tags = metadata.Tags
        //    }

        //let rehydrateEvent persistenceId eventNumber (metadata: EventMetadata) (event: obj) =
        //    let persistent = Persistent(event, eventNumber + 1L, persistenceId, metadata.EventType, false, metadata.Sender)
        //    persistent :> IPersistentRepresentation

        //let toPersistentRepresentation (resolvedEvent: ResolvedEvent) =
        //    let event, metadata = deserializeEvent resolvedEvent
        //    rehydrateEvent resolvedEvent.Event.EventStreamId resolvedEvent.Event.EventNumber metadata event

        //let writeSnapshot (snapshot: JournalSnapshot) =
        //    operation {
        //        let snapshotMetadata = snapshot |> serializeSnapshotMetadata
        //        let eventData = EventData(Guid.NewGuid(), snapshot.Manifest, true, snapshot.Snapshot |> box |> toJsonBytes, snapshotMetadata) 
        //        return! eventStore.AppendToStreamAsync(getSnapshotStream snapshot.PersistenceId, ExpectedVersion.Any |> int64, plugin.Credentials, eventData)
        //    }

        //let rec findSnapshotMetadata (criteria: SnapshotSelectionCriteria) persistenceId startIndex =
        //    operation {
        //        let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(getSnapshotStream persistenceId, startIndex, readBatchSize, true, userCredentials = plugin.Credentials)
        //        let metadataFound = 
        //            eventSlice.Events
        //            |> Seq.map (fun event -> (event, deserializeSnapshotMetadata event))
        //            |> Seq.tryFind (fun (event, metadata) -> metadata.SequenceNumber <= criteria.MaxSequenceNr && metadata.Timestamp <= criteria.MaxTimeStamp)
        //        match metadataFound with
        //        | Some (event, metadata) -> return (metadata, event.Event.Data |> parseJsonBytes<obj>) |> Some
        //        | None -> 
        //            let lastEvent = if eventSlice.Events |> isNull || eventSlice.Events.Length = 0
        //                            then -1L
        //                            else (eventSlice.Events |> Seq.last |> fun e -> e.Event.EventNumber)
        //            if lastEvent <= 0L
        //            then return None
        //            else return! findSnapshotMetadata criteria persistenceId lastEvent
        //    }

        //let findSnapshot criteria persistenceId =
        //    operation {
        //        return! findSnapshotMetadata criteria persistenceId -1L (*end*)            
        //    }

//        {new IStreamingEventJournal with
//            member __.GetCurrentPersistenceIds () =
//                operation {                       
//                    let! response = Kafka.metadata kafka <| MetadataRequest([||])
//                    let persistenceIds = response.topicMetadata |> Array.map (fun metadata -> metadata.topicName) |> Array.filter (fun name -> name.StartsWith("_") |> not) |> Set.ofArray
//                    return! Result.success persistenceIds
//                }
//            member __.PersistEvents eventsToPersist =
//                operation {
//                    let! results =
//                        eventsToPersist 
//                        |> Seq.groupBy (fun event -> event.PersistenceId)
//                        |> Seq.map (fun (persistenceId, events) -> publishEvents persistenceId events)
//                        |> Operation.Parallel

//                    let result = 
//                        results |> Array.reduce (fun acc cur -> 
//                            match cur with
//                            | Success success -> 
//                                match acc with
//                                | Success accs -> Result.successWithEvents (accs.Result |> Array.append success.Result) (accs.Events @ success.Events)
//                                | Failure errors -> Failure <| errors @ success.Events
//                            | Failure errors -> 
//                                Failure <| acc.Events @ errors)

//                    match result with
//                    | Success _ -> return! Result.successWithEvents () [PersistedSuccessfully]
//                    | Failure errors -> return! Result.failure [PersistenceError <| (AggregateException(errors) :> exn)]
//                }
//            member __.DeleteEvents persistenceId upperLimit =
//                operation {
//                    let! consumer = getConsumer persistenceId
//                    let! reuslt = Consumer.commitOffsets consumer [|(0, upperLimit)|]
//                    return! Result.successWithEvents () [DeletedSuccessfully]
//                }
//            member __.GetMaxSequenceNumber persistenceId from =
//                operation {
//                    let! consumer = getConsumer persistenceId
//                    let! offsets = Kafka.offsetFetch kafka <| OffsetFetchRequest(plugin.ConsumerGroup, [|(persistenceId, [|0|])|])
//                    let (_,maxOffset,_,_) = offsets.topics |> Array.head |> snd |> Array.head
//                    let! messages = Consumer.streamRange consumer <| Map.ofList [(0, (from, maxOffset))]
//                    let lastMessage = messages 
//                                      |> Array.collect (fun messages -> messages.messageSet.messages) 
//                                      |> Array.map (fun message -> message.message.value |> deserializeEvent) 
//                                      |> Array.maxBy (fun event -> event.SequenceNumber)
//                    return! Result.success lastMessage.SequenceNumber
//                } |> Operation.mapEither (fun sequenceNumber -> Some sequenceNumber) (fun _ -> None)
//            member __.GetEvents persistenceId first last max =
//                operation {
//                    let! consumer = getConsumer persistenceId
//                    let! messages = Consumer.streamRange consumer <| Map.ofList [(0, (first, last))]
//                    let events = messages 
//                                 |> Array.collect (fun messageSet -> messageSet.messageSet.messages) 
//                                 |> Array.map (fun message -> message.message.value |> deserializeEvent)
//                                 |> Seq.cast<IPersistentRepresentation>
//                    return! Result.success events
//                }
//            member __.GetTaggedEvents tag lowOffset highOffset =
//                operation {
//                    let! metadata = Kafka.metadata kafka <| MetadataRequest([||])
//                    let persistenceIds = metadata.topicMetadata |> Array.map (fun tm -> tm.topicName)
//                    let inline getTaggedEvents persistenceId = 
//                        operation {
//                            let! consumer = getConsumer persistenceId
//                            let first = match lowOffset with
//                                        | Some low -> low
//                                        | None -> 0L
//                            let last = match highOffset with
//                                       | Some high -> high
//                                       | None -> Int64.MaxValue
//                            let! messages = Consumer.streamRange consumer <| Map.ofList [(0, (first, last))]
//                            let events = messages 
//                                         |> Array.collect (fun messageSet -> messageSet.messageSet.messages) 
//                                         |> Array.map (fun message -> message, message.message.value |> deserializeEvent)
//                                         |> Array.filter (fun (_,event) -> event.Tags |> Seq.contains tag)
//                                         |> Seq.map (fun (message, event) -> {Id = message.offset; Tag = tag; Event = event})
//                            return! Result.success events
//                        }
//                    let results = persistenceIds |> Array.map getTaggedEvents |> Operation.Parallel |> Async.RunSynchronously
//                    let events = 
//                        seq {
//                            for result in results do
//                                match result with
//                                | Success taggedEvents -> yield! taggedEvents.Result
//                                | Failure _ -> ()
//                        }
//                    return! Result.success events
//                }
//            member __.SaveSnapshot snapshot =
//                operation {            
//                    let! result = writeSnapshot snapshot
//                    return! Result.successWithEvents () [PersistedSuccessfully]
//                }
//            member __.GetSnapshot persistenceId criteria =
//                operation {
//                    try
//                        let! metadataResult = findSnapshot criteria persistenceId
//                        return!
//                            match metadataResult with
//                            | Some (metadata, snapshot) ->
//                                 { PersistenceId = persistenceId; 
//                                   Manifest = snapshot.GetType().FullName; 
//                                   SequenceNumber = metadata.SequenceNumber; 
//                                   Timestamp = metadata.Timestamp; 
//                                   Snapshot = snapshot
//                                 } |> Some
//                            | None -> None                            
//                            |> Result.success
//                    with | _ -> 
//                        return! None |> Result.success
//                }
//            member __.DeleteSnapshots persistenceId criteria =
//                operation {
//                    let stream = getSnapshotStream persistenceId
//                    let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(stream, -1L, readBatchSize, true, userCredentials = plugin.Credentials)

//                    let upperLimit =
//                        eventSlice.Events
//                        |> Seq.map (fun event -> event, deserializeSnapshotMetadata event)
//                        |> Seq.filter (fun (_,metadata) -> metadata.SequenceNumber >= criteria.MinSequenceNr && metadata.SequenceNumber <= criteria.MaxSequenceNr && metadata.Timestamp >= criteria.MinTimestamp.GetValueOrDefault() && metadata.Timestamp = criteria.MaxTimeStamp)
//                        |> Seq.maxBy (fun (event,_) -> event.Event.EventNumber)
//                        |> fun (event,_) -> event.Event.EventNumber

//                    let! metadataResult = eventStore.GetStreamMetadataAsync(stream, plugin.Credentials)
//                    let metadata = metadataResult.StreamMetadata
//                    let newMetadata = StreamMetadata.Create(metadata.MaxCount, metadata.MaxAge, upperLimit |> Nullable, metadata.CacheControl, metadata.Acl)
//                    let! result = eventStore.SetStreamMetadataAsync(stream, metadataResult.MetastreamVersion, newMetadata, plugin.Credentials)

//                    return! Result.successWithEvents () [DeletedSuccessfully]
//                }
//            member __.DeleteAllSnapshots persistenceId sequenceNumber =
//                operation {
//                    let stream = getSnapshotStream persistenceId
//                    let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(stream, -1L, readBatchSize, true, userCredentials = plugin.Credentials)

//                    let upperLimit =
//                        eventSlice.Events
//                        |> Seq.map (fun event -> event, deserializeSnapshotMetadata event)
//                        |> Seq.filter (fun (_,metadata) -> metadata.SequenceNumber <= sequenceNumber)
//                        |> Seq.maxBy (fun (event,_) -> event.Event.EventNumber)
//                        |> fun (event,_) -> event.Event.EventNumber

//                    let! metadataResult = eventStore.GetStreamMetadataAsync(stream, plugin.Credentials)
//                    let metadata = metadataResult.StreamMetadata
//                    let newMetadata = StreamMetadata.Create(metadata.MaxCount, metadata.MaxAge, upperLimit |> Nullable, metadata.CacheControl, metadata.Acl)
//                    let! result = eventStore.SetStreamMetadataAsync(stream, metadataResult.MetastreamVersion, newMetadata, plugin.Credentials)

//                    return! Result.successWithEvents () [DeletedSuccessfully]
//                }
//            member __.SubscribeToEvents persistenceId fromSequence =
//                {new IPublisher<JournaledEvent> with
//                    member __.Subscribe subscriber = 
//                        eventStore.SubscribeToStreamFrom(persistenceId, fromSequence |> Nullable, catchUpSettings, (fun _ event -> event |> getJournaledEvent |> subscriber.OnNext), userCredentials = plugin.Credentials)
//                        |> ignore
//                }
//            member __.SubscribeToPersistenceIds () =
//                {new IPublisher<string> with
//                    member __.Subscribe subscriber = 
//                        let notifyEvent (event: ResolvedEvent) =
//                            if event.Event |> isNotNull
//                            then event.Event.EventStreamId |> subscriber.OnNext

//                        eventStore.SubscribeToStreamFrom("$streams", Nullable 0L, catchUpSettings, (fun _ event -> notifyEvent event), userCredentials = plugin.Credentials)
//                        |> ignore
//                }
//        }

//type KafkaProvider() =
//    interface IStreamingEventJournalProvider with
//        member __.GetEventJournal config context = EventJournal.get config context.System :> IEventJournal
//        member __.GetStreamingEventJournal config system = EventJournal.get config system

//type KafkaJournal (config: Config) =
//    inherit StreamingEventJournal<KafkaProvider>(config)
//    static member Identifier = "akka.persistence.journal.kafka"

//type KafkaSnapshotStore (config: Config) =
//    inherit SnapshotStoreBase<KafkaProvider>(config)
//    static member Identifier = "akka.persistence.snapshot-store.kafka"

