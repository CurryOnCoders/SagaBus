namespace Akka.Persistence.Elasticsearch

open Akka.Actor
open Akka.Persistence.Query
open CurryOn.Akka

type ElasticsearchReadJournal (system: ExtendedActorSystem) =        
    inherit QueryReadJournalBase(system, system.Settings.Config.GetConfig(ElasticsearchReadJournal.Identifier), ElasticsearchJournal.Identifier)
    static member Identifier = "akka.persistence.query.journal.elasticsearch"

type ElasticsearchReadJournalProvider (system: ExtendedActorSystem) =
    interface IReadJournalProvider with
        member __.GetReadJournal () = ElasticsearchReadJournal(system) :> IReadJournal

