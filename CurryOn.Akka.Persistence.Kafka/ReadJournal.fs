namespace Akka.Persistence.Kafka

open Akka.Actor
open Akka.Persistence.Query
open CurryOn.Akka

// TODO: uncomment when the KafkaProvider is ready.
//type KafkaReadJournal (system: ExtendedActorSystem) =        
//    inherit StreamingQueryReadJournalBase<KafkaProvider>(system, system.Settings.Config.GetConfig(KafkaReadJournal.Identifier), KafkaJournal.Identifier)
//    static member Identifier = "akka.persistence.query.journal.kafka"

//type KafkaReadJournalProvider (system: ExtendedActorSystem) =
//    interface IReadJournalProvider with
//        member __.GetReadJournal () = KafkaReadJournal(system) :> IReadJournal
        

