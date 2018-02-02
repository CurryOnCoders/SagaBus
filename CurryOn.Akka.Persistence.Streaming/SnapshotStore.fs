namespace CurryOn.Akka

open Akka.Actor
open Akka.Configuration
open Akka.Persistence
open Akka.Persistence.Snapshot
open CurryOn.Akka
open CurryOn.Common
open FSharp.Control
open System
open System.Threading.Tasks

[<AbstractClass>]
type StreamingSnapshotStore<'provider when 'provider :> IEventJournalProvider and 'provider: (new: unit -> 'provider)> (config: Config) =
    inherit SnapshotStore()
    let context = SnapshotStore.Context
    let provider = new 'provider() :> IEventJournalProvider
    let writeJournal = provider.GetEventJournal config context  

    override __.SaveAsync (metadata, snapshot) =
        task {
            let journalSnapshot =
                { PersistenceId = metadata.PersistenceId
                  SequenceNumber = metadata.SequenceNr
                  Manifest = snapshot.GetType().FullName
                  Timestamp = metadata.Timestamp
                  Snapshot = snapshot
                }
            
            let result = writeJournal.SaveSnapshot journalSnapshot |> Operation.wait

            return ()
        } :> Task

    override __.LoadAsync (persistenceId, criteria) =
        task {            
            let! result = writeJournal.GetSnapshot persistenceId criteria |> PersistenceOperation.toTask
            match result with
            | Some snapshot -> return SelectedSnapshot(SnapshotMetadata(snapshot.PersistenceId, snapshot.SequenceNumber, snapshot.Timestamp), snapshot.Snapshot)
            | None -> return null
        }
    
    override __.DeleteAsync (metadata) =
        task {
            let persistenceOperation = 
                if metadata.Timestamp > DateTime.MinValue
                then SnapshotSelectionCriteria(metadata.SequenceNr, metadata.Timestamp) 
                     |> writeJournal.DeleteSnapshots metadata.PersistenceId
                else writeJournal.DeleteAllSnapshots metadata.PersistenceId metadata.SequenceNr

            return! persistenceOperation |> PersistenceOperation.toTask
        } :> Task

    override __.DeleteAsync (persistenceId, criteria) =
        task {
            return! writeJournal.DeleteSnapshots persistenceId criteria |> PersistenceOperation.toTask
        } :> Task