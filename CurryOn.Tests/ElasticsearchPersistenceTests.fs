namespace CurryOn.Tests

open Akka.Actor
open Akka.FSharp
open Microsoft.VisualStudio.TestTools.UnitTesting

[<TestClass>]
type ElasticsearchPersistenceTests () =
    static let actorSystem = ref <| Unchecked.defaultof<ActorSystem>
    static let sleep seconds = (seconds * 1000) |> System.Threading.Thread.Sleep

    [<ClassInitialize>]
    static member InitializeActorSystem (_: TestContext) =
        let hocon = """akka {
            actor {
              loglevel = DEBUG
              loggers = ["CurryOn.Tests.DebugOutputLogger, CurryOn.Tests"]
              provider = "Akka.Actor.LocalActorRefProvider, Akka"
              serializers {
                hyperion = "Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion"
              }
              serialization-bindings {
                "System.Object" = hyperion
              }
            }
            persistence {
              journal {
                plugin = "akka.persistence.journal.elasticsearch"
                elasticsearch {
                    class = "Akka.Persistence.Elasticsearch.ElasticsearchJournal, CurryOn.Akka.Persistence.Elasticsearch"
                    uri = "http://localhost:9200"
                    write-batch-size = 4095
                    read-batch-size = 4095
                    recreate-indices = false
                    index-mappings {
                        "event_journal" = "Akka.Persistence.Elasticsearch.PersistedEvent, CurryOn.Akka.Persistence.Elasticsearch"
                        "metadata_store" = "Akka.Persistence.Elasticsearch.EventJournalMetadata, CurryOn.Akka.Persistence.Elasticsearch"
                        "snapshot_store" = "Akka.Persistence.Elasticsearch.Snapshot, CurryOn.Akka.Persistence.Elasticsearch"
                    }
                }
              }
              snapshot-store {
                plugin = "akka.persistence.snapshot-store.elasticsearch"
                elasticsearch {
                    class = "Akka.Persistence.Elasticsearch.ElasticsearchSnapshotStore, CurryOn.Akka.Persistence.Elasticsearch"
                    uri = "http://localhost:9200"
                    read-batch-size = 4095
                    index-mappings {
                        "event_journal" = "Akka.Persistence.Elasticsearch.PersistedEvent, CurryOn.Akka.Persistence.Elasticsearch"
                        "metadata_store" = "Akka.Persistence.Elasticsearch.EventJournalMetadata, CurryOn.Akka.Persistence.Elasticsearch"
                        "snapshot_store" = "Akka.Persistence.Elasticsearch.Snapshot, CurryOn.Akka.Persistence.Elasticsearch"
                    }
                }
              }
            }
          }"""

        let configuration = Configuration.parse hocon
        actorSystem := System.create "company" configuration
        actorSystem.Value.Settings.Loggers.Add(typeof<DebugOutputLogger>.AssemblyQualifiedName)
        

    [<TestMethod()>]
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