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
    let getStream persistenceId = sprintf "snapshot-%s" persistenceId

    override __.SaveAsync (metadata, snapshot) =
        let eventMetadata = {PersistenceId = metadata.PersistenceId; SequenceNumber = metadata.SequenceNr; Timestamp = metadata.Timestamp} |> serializer.ToBinary
        let eventData = EventData(Guid.NewGuid(), snapshot.GetType().FullName, true, serializer.ToBinary snapshot, eventMetadata) 
        eventStore.AppendToStreamAsync(getStream metadata.PersistenceId, ExpectedVersion.Any |> int64, plugin.Credentials, eventData) :> Task        

    override __.LoadAsync (persistenceId, criteria) =
        task {
            let! eventSlice = eventStore.ReadStreamEventsBackwardAsync(getStream persistenceId, -1L (*end*), readBatchSize.Value, true, userCredentials = plugin.Credentials)
            let snapshotEvent, metadata = 
                eventSlice.Events 
                |> Seq.map (fun resolvedEvent -> (resolvedEvent, resolvedEvent.Event.Metadata |> serializer.FromBinary<SnapshotMetadata>))
                |> Seq.find (fun (_, metadata) -> metadata.SequenceNumber <= criteria.MaxSequenceNr && metadata.Timestamp <= criteria.MaxTimeStamp)

            let clrType = snapshotEvent |> EventJournal.getEventType
            return SelectedSnapshot(Akka.Persistence.SnapshotMetadata(persistenceId, metadata.SequenceNumber, metadata.Timestamp), serializer.FromBinary(snapshotEvent.Event.Data, clrType))
        }