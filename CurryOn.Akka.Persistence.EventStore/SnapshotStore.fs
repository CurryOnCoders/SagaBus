namespace Akka.Persistence.EventStore

open Akka.Actor
open Akka.Persistence
open Akka.Persistence.Snapshot
open CurryOn.Common
open EventStore.ClientAPI
open System
open System.Threading.Tasks

[<CLIMutable>]
type SnapshotMetadata =
    {
        PersistenceId: string
        SequenceNumber: int64
        Timestamp: DateTime
    }

type EventStoreSnapshotStore (context: IActorContext) =
    inherit SnapshotStore()
    let plugin = EventStorePlugin(context)
    let config = lazy(context.System.Settings.Config.GetConfig("eventstore.persistence.journal"))
    let readBatchSize = lazy(config.Value.GetInt("read-batch-size"))
    let eventStore = plugin.Connect()
    let serializer = EventStoreSerializer(context.System :?> ExtendedActorSystem)
    let getMetadataStream persistenceId = sprintf "snapshots-%s" persistenceId
    let getStream persistenceId version = sprintf "snapshot-%s-%d" persistenceId version
    let toMetadata (resolvedEvent: ResolvedEvent) = resolvedEvent.Event.Data |> serializer.FromBinary<SnapshotMetadata>

    let addSnapshotToMetadataLog (metadata: Akka.Persistence.SnapshotMetadata) =
        task {
            let logStream = getMetadataStream metadata.PersistenceId
            let eventMetadata = {PersistenceId = metadata.PersistenceId; SequenceNumber = metadata.SequenceNr; Timestamp = metadata.Timestamp} |> serializer.ToBinary
            let eventData = EventData(Guid.NewGuid(), typeof<SnapshotMetadata>.Name, true, eventMetadata, [||])
            let! writeResult = eventStore.AppendToStreamAsync(logStream, ExpectedVersion.Any |> int64, plugin.Credentials, eventData)
            return eventMetadata
        }

    let writeSnapshot metadata snapshot =
        task {
            let! eventMetadata = addSnapshotToMetadataLog metadata
            let eventData = EventData(Guid.NewGuid(), snapshot.GetType().FullName, true, serializer.ToBinary snapshot, eventMetadata) 
            return! eventStore.AppendToStreamAsync(getStream metadata.PersistenceId metadata.SequenceNr, ExpectedVersion.Any |> int64, plugin.Credentials, eventData)
        }

    let rec findSnapshotMetadata (criteria: SnapshotSelectionCriteria) persistenceId startIndex =
        task {
            let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(getMetadataStream persistenceId, startIndex, readBatchSize.Value, true, userCredentials = plugin.Credentials)
            let metadataFound = 
                eventSlice.Events
                |> Seq.map toMetadata
                |> Seq.tryFind (fun metadata -> metadata.SequenceNumber <= criteria.MaxSequenceNr && metadata.Timestamp <= criteria.MaxTimeStamp)
            match metadataFound with
            | Some metadata -> return metadata
            | None -> return! findSnapshotMetadata criteria persistenceId (eventSlice.Events |> Seq.last |> fun e -> e.Event.EventNumber)
        }

    let findSnapshot criteria persistenceId =
        task {
            let! snapshotMetadata = findSnapshotMetadata criteria persistenceId -1L (*end*)
            let snapshotStream = getStream snapshotMetadata.PersistenceId snapshotMetadata.SequenceNumber
            let! snapshotReadResult = eventStore.ReadEventAsync(snapshotStream, StreamPosition.End |> int64, true, userCredentials = plugin.Credentials) 
            let snapshotEvent = snapshotReadResult.Event.Value
            let clrType = snapshotEvent |> EventJournal.getEventType
            return (snapshotMetadata, serializer.FromBinary(snapshotEvent.Event.Data, clrType))
        }

    override __.SaveAsync (metadata, snapshot) =
        writeSnapshot metadata snapshot |> Task.Ignore |> Task.fromUnit      

    override __.LoadAsync (persistenceId, criteria) =
        task {
            let! (metadata, snapshot) = findSnapshot criteria persistenceId
            return SelectedSnapshot(Akka.Persistence.SnapshotMetadata(persistenceId, metadata.SequenceNumber, metadata.Timestamp), snapshot)
        }
    
    override __.DeleteAsync (metadata) =
        task {
            if metadata.Timestamp = DateTime.MinValue
            then return! eventStore.DeleteStreamAsync(getStream metadata.PersistenceId metadata.SequenceNr, ExpectedVersion.Any |> int64, userCredentials = plugin.Credentials) |> Task.Ignore
            else let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(getMetadataStream metadata.PersistenceId, -1L, readBatchSize.Value, true, userCredentials = plugin.Credentials)
                 return! eventSlice.Events
                         |> Seq.map toMetadata
                         |> Seq.filter (fun snapshotMetadata -> snapshotMetadata.SequenceNumber <= metadata.SequenceNr && snapshotMetadata.Timestamp = metadata.Timestamp)
                         |> Seq.map (fun snapshotMetadata -> eventStore.DeleteStreamAsync(getStream snapshotMetadata.PersistenceId snapshotMetadata.SequenceNumber, ExpectedVersion.Any |> int64, userCredentials = plugin.Credentials))
                         |> Task.Parallel
                         |> Task.Ignore
        } :> Task

    override __.DeleteAsync (persistenceId, criteria) =
        task {
            let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(getMetadataStream persistenceId, -1L, readBatchSize.Value, true, userCredentials = plugin.Credentials)
            return! eventSlice.Events
                    |> Seq.map toMetadata
                    |> Seq.filter (fun metadata -> metadata.SequenceNumber >= criteria.MinSequenceNr && metadata.SequenceNumber <= criteria.MaxSequenceNr && metadata.Timestamp >= criteria.MinTimestamp.GetValueOrDefault() && metadata.Timestamp = criteria.MaxTimeStamp)
                    |> Seq.map (fun metadata -> eventStore.DeleteStreamAsync(getStream metadata.PersistenceId metadata.SequenceNumber, ExpectedVersion.Any |> int64, userCredentials = plugin.Credentials))
                    |> Task.Parallel
                    |> Task.Ignore
        } :> Task