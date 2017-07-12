namespace Akka.Persistence.EventStore

open Akka.Actor
open Akka.Persistence
open Akka.Streams
open CurryOn.Akka.EventStore

type IEventStorePlugin =
    inherit IJournalPlugin

type internal EventStorePlugin (system: ActorSystem) =
    let settings = Settings(system, system.Settings.Config)
    new (context: IActorContext) = EventStorePlugin(context.System)    
    member __.Connect () = EventStore.store.Value
    member __.Config = system.Settings.Config
    member __.Serialization = EventStoreSerialization(system)
    member __.Materializer = ActorMaterializer.Create(system)
    member __.Credentials = EventStore.credentials.Value
