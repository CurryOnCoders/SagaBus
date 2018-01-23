namespace CurryOn.Tests

open Akka.Actor
open Akka.FSharp
open Akka.Persistence.Elasticsearch
open Akka.Persistence.Query
open Akka.Streams.Dsl
open CurryOn.Elastic
open FSharp.Control
open Microsoft.VisualStudio.TestTools.UnitTesting
open System
open CurryOn.Common

[<TestClass>]
type ElasticsearchPersistenceTests () =
    static let actorSystem = ref <| Unchecked.defaultof<ActorSystem>
    static let sleep seconds = (seconds * 1000) |> System.Threading.Thread.Sleep
    static let mappings =
        [{Type = typeof<PersistedEvent>; IndexName = "event_journal"; TypeName = "persisted_event"};
         {Type = typeof<Snapshot>; IndexName = "snapshot_store"; TypeName = "snapshot"};
         {Type = typeof<EventJournalMetadata>; IndexName = "metadata_store"; TypeName = "event_journal_metadata"}]

    let client = Elasticsearch.connect {Node = Uri "http://localhost:9200"; DisableDirectStreaming = false; RequestTimeout = TimeSpan.FromMinutes 1.0; IndexMappings = mappings}

    [<ClassInitialize>]
    static member InitializeActorSystem (_: TestContext) =
        let hocon = 
            """akka 
               {
                actor 
                {
                  loglevel = DEBUG
                  loggers = ["CurryOn.Tests.DebugOutputLogger, CurryOn.Tests"]
                  provider = "Akka.Actor.LocalActorRefProvider, Akka"
                  serializers 
                  {
                    hyperion = "Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion"
                  }
                  serialization-bindings 
                  {
                    "System.Object" = hyperion
                  }
                }
                persistence
                {
                  journal 
                  {
                    plugin = "akka.persistence.journal.elasticsearch"
                    elasticsearch 
                    {
                        class = "Akka.Persistence.Elasticsearch.ElasticsearchJournal, CurryOn.Akka.Persistence.Elasticsearch"
                        uri = "http://localhost:9200"
                        write-batch-size = 4095
                        read-batch-size = 4095
                        recreate-indices = false
                    }
                  }
                  snapshot-store 
                  {
                    plugin = "akka.persistence.snapshot-store.elasticsearch"
                    elasticsearch 
                    {
                        class = "Akka.Persistence.Elasticsearch.ElasticsearchSnapshotStore, CurryOn.Akka.Persistence.Elasticsearch"
                        uri = "http://localhost:9200"
                        read-batch-size = 4095
                    }
                  }              
                  query 
                  {
                    journal 
                    {
                      elasticsearch 
                      {
                          class = "Akka.Persistence.Elasticsearch.ElasticsearchReadJournalProvider, CurryOn.Akka.Persistence.Elasticsearch"
                          uri = "http://localhost:9200"
                          read-batch-size = 4095
                      }
                    }  
                  }
                }
              }"""

        let configuration = Configuration.parse hocon
        let queryConfig = configuration.GetConfig(ElasticsearchReadJournal.Identifier)
        actorSystem := System.create "company" configuration
        actorSystem.Value.Settings.Loggers.Add(typeof<DebugOutputLogger>.AssemblyQualifiedName)
        

    [<TestMethod>]
    member __.TestElasticsearchPersistence () =
        let akka = actorSystem.Value
        let employees = akka.ActorOf<EmployeesActor>("all-employees-1")

        employees <! {Name = "Jim Gnomes"; Position = "Operator Overloader"; Salary = 32044.56M}
        employees <! {Name = "Rob Hobbit"; Position = "Currier"; Salary = 54862.95M}
        employees <! {Name = "Gerald Munk"; Position = "Lambda Invoker"; Salary = 48350.85M}

        sleep 3
        employees <! TakeSnapshot
        sleep 2

        let (allEmployees: Employee list) = employees <? GetEmployees |> Async.RunSynchronously
        Assert.AreEqual(3, allEmployees.Length)

        let jim = allEmployees |> List.find (fun e -> e.Name.StartsWith("Jim"))
        Assert.AreEqual(32044.56M, jim.Salary)

        sleep 3
        employees <! {Id = jim.Id; Salary = 46044.56M}
        sleep 2
        employees <! PoisonPill.Instance

        sleep 1
        let persistedEmployees = akka.ActorOf<EmployeesActor>("all-employees-2")

        sleep 2
        let (fetchedEmployees: Employee list) = persistedEmployees <? GetEmployees |> Async.RunSynchronously

        Assert.AreEqual(allEmployees.Length, fetchedEmployees.Length)
        let persistedJim = fetchedEmployees |> List.find (fun e -> e.Name.StartsWith("Jim"))
        Assert.AreEqual(46044.56M, persistedJim.Salary)

    [<TestMethod>]
    member __.TestElasticsearchReadJouranal () =
        let akka = actorSystem.Value
        let materializer = Akka.Streams.ActorMaterializer.Create(akka)
        let readJournal = PersistenceQuery.Get(akka).ReadJournalFor<ElasticsearchReadJournal>(ElasticsearchReadJournal.Identifier)
        let employees = new System.Collections.Generic.List<string>()        
        
        readJournal.CurrentPersistenceIds().RunForeach((fun id -> employees.Add(id)), materializer) |> Task.ofUnit |> Task.runSynchronously
        Assert.IsTrue(employees.Count > 0)

        for persistenceId in employees do
            let events = new System.Collections.Generic.List<EventEnvelope>()
            readJournal.CurrentEventsByPersistenceId(persistenceId, 0L, Int64.MaxValue).RunForeach((fun event -> events.Add(event)), materializer) |> Task.ofUnit |> Task.runSynchronously
            Assert.IsTrue(events.Count > 0)