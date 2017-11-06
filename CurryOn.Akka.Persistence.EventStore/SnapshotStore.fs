namespace Akka.Persistence.EventStore

open Akka.Actor
open Akka.Configuration
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

type EventStoreSnapshotStore (config: Config) =
    inherit SnapshotStore()
    let context = SnapshotStore.Context
    let plugin = EventStorePlugin(context)
    //let config = lazy(context.System.Settings.Config.GetConfig("akka.persistence.snapshot-store.event-store"))
    let readBatchSize = lazy(config.GetInt("read-batch-size"))
    let connect () = plugin.Connect()
    let getMetadataStream persistenceId = sprintf "snapshots-%s" persistenceId
    let getStream persistenceId version = sprintf "snapshot-%s-%d" persistenceId version
    let toMetadata (resolvedEvent: ResolvedEvent) = resolvedEvent.Event.Data |> Serialization.parseJsonBytes<SnapshotMetadata>

    let addSnapshotToMetadataLog (metadata: Akka.Persistence.SnapshotMetadata) =
        task {
            let! eventStore = connect()
            let logStream = getMetadataStream metadata.PersistenceId
            let eventMetadata = {PersistenceId = metadata.PersistenceId; SequenceNumber = metadata.SequenceNr; Timestamp = metadata.Timestamp} |> Serialization.toJsonBytes
            let eventData = EventData(Guid.NewGuid(), typeof<SnapshotMetadata>.Name, true, eventMetadata, [||])
            let! writeResult = eventStore.AppendToStreamAsync(logStream, ExpectedVersion.Any |> int64, plugin.Credentials, eventData)
            return eventMetadata
        }

    let writeSnapshot metadata snapshot =
        task {
            let! eventStore = connect()
            let! eventMetadata = addSnapshotToMetadataLog metadata
            let eventData = EventData(Guid.NewGuid(), snapshot.GetType().FullName, true, Serialization.toJsonBytes snapshot, eventMetadata) 
            return! eventStore.AppendToStreamAsync(getStream metadata.PersistenceId metadata.SequenceNr, ExpectedVersion.Any |> int64, plugin.Credentials, eventData)
        }

    let rec findSnapshotMetadata (criteria: SnapshotSelectionCriteria) persistenceId startIndex =
        task {
            let! eventStore = connect()
            let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(getMetadataStream persistenceId, startIndex, readBatchSize.Value, true, userCredentials = plugin.Credentials)
            let metadataFound = 
                eventSlice.Events
                |> Seq.map toMetadata
                |> Seq.tryFind (fun metadata -> metadata.SequenceNumber <= criteria.MaxSequenceNr && metadata.Timestamp <= criteria.MaxTimeStamp)
            match metadataFound with
            | Some metadata -> return metadata |> Some
            | None -> 
                let lastEvent = if eventSlice.Events |> isNull || eventSlice.Events.Length = 0
                                then -1L
                                else (eventSlice.Events |> Seq.last |> fun e -> e.Event.EventNumber)
                if lastEvent <= 0L
                then return None
                else return! findSnapshotMetadata criteria persistenceId lastEvent
        }

    let findSnapshot criteria persistenceId =
        task {
            let! eventStore = connect()
            let! snapshotMetadata = findSnapshotMetadata criteria persistenceId -1L (*end*)
            match snapshotMetadata with
            | Some metadata ->
                let snapshotStream = getStream metadata.PersistenceId metadata.SequenceNumber
                let! snapshotReadResult = eventStore.ReadEventAsync(snapshotStream, StreamPosition.End |> int64, true, userCredentials = plugin.Credentials) 
                let snapshotEvent = snapshotReadResult.Event.Value
                let clrType = snapshotEvent |> EventJournal.getEventType
                return (metadata, Serialization.parseJsonBytesAs snapshotEvent.Event.Data clrType) |> Some
            | None -> return None            
        }

    override __.SaveAsync (metadata, snapshot) =
        writeSnapshot metadata snapshot |> Task.Ignore |> Task.fromUnit      

    override __.LoadAsync (persistenceId, criteria) =
        task {
            try
                let! metadataResult = findSnapshot criteria persistenceId
                match metadataResult with
                | Some (metadata, snapshot) ->
                    return SelectedSnapshot(Akka.Persistence.SnapshotMetadata(persistenceId, metadata.SequenceNumber, metadata.Timestamp), snapshot)
                | None ->
                    return null
            with | ex ->
                return null
        }
    
    override __.DeleteAsync (metadata) =
        task {
            let! eventStore = connect()
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
            let! eventStore = connect()
            let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(getMetadataStream persistenceId, -1L, readBatchSize.Value, true, userCredentials = plugin.Credentials)
            return! eventSlice.Events
                    |> Seq.map toMetadata
                    |> Seq.filter (fun metadata -> metadata.SequenceNumber >= criteria.MinSequenceNr && metadata.SequenceNumber <= criteria.MaxSequenceNr && metadata.Timestamp >= criteria.MinTimestamp.GetValueOrDefault() && metadata.Timestamp = criteria.MaxTimeStamp)
                    |> Seq.map (fun metadata -> eventStore.DeleteStreamAsync(getStream metadata.PersistenceId metadata.SequenceNumber, ExpectedVersion.Any |> int64, userCredentials = plugin.Credentials))
                    |> Task.Parallel
                    |> Task.Ignore
        } :> Task