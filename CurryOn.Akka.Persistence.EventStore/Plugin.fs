namespace Akka.Persistence.EventStore

open Akka.Actor
open Akka.Persistence
open Akka.Streams
open CurryOn.Akka
open EventStore.ClientAPI.SystemData

type IEventStorePlugin =
    inherit IJournalPlugin

type internal EventStorePlugin (system: ActorSystem) =
    let config = system.Settings.Config
    let settings = config.GetConfig("akka.persistence.journal.event-store") |> Settings.Load
    let connection = settings |> EventStoreConnection.connect
    new (context: IActorContext) = EventStorePlugin(context.System)    
    member __.Connect () = connection
    member __.Config = config
    member __.Materializer = ActorMaterializer.Create(system)
    member __.Credentials = UserCredentials(settings.UserName, settings.Password)
