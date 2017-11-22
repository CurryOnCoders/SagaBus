namespace CurryOn.Tests

open Akka.Actor
open Akka.FSharp
open Microsoft.VisualStudio.TestTools.UnitTesting

[<TestClass>]
type EventStorePersistenceTests () =
    static let actorSystem = ref <| Unchecked.defaultof<ActorSystem>
    static let sleep seconds = (seconds * 1000) |> System.Threading.Thread.Sleep

    [<ClassInitialize>]
    static member InitializeActorSystem (_: TestContext) =
        let hocon = """akka {
            actor {
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
                plugin = "akka.persistence.journal.event-store"
                event-store {
                    class = "Akka.Persistence.EventStore.EventStoreJournal, CurryOn.Akka.Persistence.EventStore"
                    server-name = "localhost"
                    write-batch-size = 4095
                    read-batch-size = 4095
                }
              }
              snapshot-store {
                plugin = "akka.persistence.snapshot-store.event-store"
                event-store {
                    class = "Akka.Persistence.EventStore.EventStoreSnapshotStore, CurryOn.Akka.Persistence.EventStore"
                    server-name = "localhost"
                    read-batch-size = 4095
                }
              }
            }
          }"""

        actorSystem := System.create "company" <| Configuration.parse hocon

    [<TestMethod()>]
    member __.TestEventPersistence () =
        let akka = actorSystem.Value
        let employees = akka.ActorOf<EmployeesActor>("all-employees-1")

        employees <! {Name = "Jim Gnomes"; Position = "Operator Overloader"; Salary = 32044.56M}
        employees <! {Name = "Rob Hobbit"; Position = "Currier"; Salary = 54862.95M}
        employees <! {Name = "Gerald Munk"; Position = "Lambda Invoker"; Salary = 48350.85M}

        sleep 2
        employees <! TakeSnapshot
        sleep 1

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