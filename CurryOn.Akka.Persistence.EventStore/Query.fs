namespace Akka.Persistence.EventStore.Query

open Akka.Actor
open Akka.Persistence
open Akka.Persistence.EventStore
open Akka.Persistence.Query
open Akka.Streams.Dsl
open Dandh.Common
open EventStore.ClientAPI
open Reactive.Streams
open System

type EventStoreReadJournal (system: ExtendedActorSystem) =
    let serialization = EventStoreSerialization(system)
    let plugin = EventStorePlugin(system)
    static member Identifier = "eventstore.persistence.query"
    interface IReadJournal
    interface IAllPersistenceIdsQuery with
        member __.AllPersistenceIds () =
            let eventStore = plugin.Connect() |> Async.RunSynchronously
            Source.FromPublisher {new IPublisher<string> with
                                    member __.Subscribe subscriber =
                                        let notify (resolvedEvent: ResolvedEvent) =
                                            if resolvedEvent.Event |> isNotNull
                                            then subscriber.OnNext(resolvedEvent.Event.EventStreamId)
                                        eventStore.SubscribeToStreamFrom("$streams", 0L |> Nullable, CatchUpSubscriptionSettings.Default, (fun _ event -> notify event), userCredentials = !plugin.Credentials) |> ignore
                                 }



type EventStoreReadJournalProvider (system: ExtendedActorSystem) =
    interface IReadJournalProvider with
        member __.GetReadJournal () = EventStoreReadJournal(system) :> IReadJournal

