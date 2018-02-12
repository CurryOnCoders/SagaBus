namespace Akka.Persistence.Kafka

open Akka.Actor
open Akka.Persistence
open Akka.Streams
open CurryOn.Akka

type IKafkaPlugin =
    inherit IJournalPlugin

type internal KafkaPlugin (system: ActorSystem) =
    let config = system.Settings.Config
    let settings = config.GetConfig("akka.persistence.journal.kafka") |> Settings.Load
    let connect () = settings |> KafkaConnection.create
    new (context: IActorContext) = KafkaPlugin(context.System)    
    member __.Config = config
    member __.Connect () = connect ()
    member __.ConsumerGroup = settings.ClientId
    member __.Materializer = ActorMaterializer.Create(system)
