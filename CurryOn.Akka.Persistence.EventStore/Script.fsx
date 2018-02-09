//#load @"\\hbg_nt\mis\AEshbach\Shared Scripts\GenerateReferences.fsx"
//References.generateDlls __SOURCE_DIRECTORY__
//References.generateFs __SOURCE_DIRECTORY__

#r @"..\packages\Akka.1.2.3\lib\net45\Akka.dll"
#r @"..\packages\Akka.FSharp.1.2.3\lib\net45\Akka.FSharp.dll"
#r @"..\packages\Akka.Persistence.1.2.3.43-beta\lib\net45\Akka.Persistence.dll"
#r @"..\packages\Akka.Persistence.FSharp.1.2.3.43-beta\lib\net45\Akka.Persistence.FSharp.dll"
#r @"..\packages\Akka.Persistence.Query.1.2.3.43-beta\lib\net45\Akka.Persistence.Query.dll"
#r @"..\packages\Hyperion.0.9.2\lib\net45\Hyperion.dll"
#r @"..\packages\Akka.Serialization.Hyperion.1.2.3.43-beta\lib\net45\Akka.Serialization.Hyperion.dll"
#r @"..\packages\Akka.Streams.1.2.3\lib\net45\Akka.Streams.dll"
#r @"..\packages\EventStore.Client.4.0.3\lib\net40\EventStore.ClientAPI.dll"
#r @"..\packages\FsPickler.3.2.0\lib\net45\FsPickler.dll"
#r @"..\packages\FsPickler.Json.3.2.0\lib\net45\FsPickler.Json.dll"
#r @"..\packages\Google.ProtocolBuffers.2.4.1.555\lib\net40\Google.ProtocolBuffers.dll"
#r @"..\packages\Google.ProtocolBuffers.2.4.1.555\lib\net40\Google.ProtocolBuffers.Serialization.dll"
#r @"..\packages\Newtonsoft.Json.9.0.1\lib\net45\Newtonsoft.Json.dll"
#r @"..\packages\Reactive.Streams.1.0.2\lib\net45\Reactive.Streams.dll"
#r @"..\packages\System.Collections.Immutable.1.3.1\lib\portable-net45+win8+wp8+wpa81\System.Collections.Immutable.dll"
#r @"..\packages\System.ValueTuple.4.3.0\lib\netstandard1.0\System.ValueTuple.dll"
#r @"C:\Projects\GitHub\CurryOn\packages\Microsoft.VisualStudio.Threading.15.4.4\lib\net45\Microsoft.VisualStudio.Threading.dll"
#r @"../CurryOn.Common/bin/debug/CurryOn.Common.dll"
#r @"bin/debug/CurryOn.Akka.Persistence.EventStore.dll"

open Akka.Actor
open Akka.FSharp
open Akka.Persistence
open Akka.Persistence.EventStore
open Akka.Persistence.FSharp
open Akka.Routing
open CurryOn.Common
open EventStore.ClientAPI
open EventStore.ClientAPI.SystemData
open System
open System.Collections.Generic
open System.Linq.Expressions

[<CLIMutable>]
type Employee =
    {
        Id: Guid
        Name: string
        Position: string
        DateHired: DateTime
        Salary: decimal
    } member this.YearsOfService = (DateTime.Now - this.DateHired).TotalDays / 365.25 |> Math.Round |> int
      static member Empty = {Id = Guid.Empty; Name = String.Empty; Position = String.Empty; DateHired = DateTime.MinValue; Salary = 0M}
      static member Create id = {Employee.Empty with Id = id; DateHired = DateTime.Now}

[<CLIMutable>]
type CreateEmployee =
    {
        Name: string
        Position: string
        Salary: decimal
    } 

[<CLIMutable>]
type RenameEmployee =
    {
        Id: Guid
        Name: string
    } interface IConsistentHashable with
        member this.ConsistentHashKey = this.Id |> box

[<CLIMutable>]
type ChangeSalary =
    {
        Id: Guid
        Salary: decimal
    } interface IConsistentHashable with
        member this.ConsistentHashKey = this.Id |> box

[<CLIMutable>]
type EmployeeCreated =
    {
        EmployeeId: Guid
        Name: string
        Position: string
        DateHired: DateTime
        Salary: decimal
    }

[<CLIMutable>]
type EmployeeRenamed =
    {
        Name: string
    }

[<CLIMutable>]
type SalaryChanged =
    {
        Salary: decimal
    }

type SnapshotCommand = TakeSnapshot
type EmployeesCommand = 
| GetEmployees
| PrintEmployees

type EmployeeActor (employeeId) as actor =
    inherit ReceivePersistentActor ()
    let context = ActorBase.Context
    let state = ref <| Employee.Create employeeId

    do actor.Recover<SnapshotOffer>(fun (offer: SnapshotOffer) -> state := offer.Snapshot |> unbox)
    do actor.Recover<EmployeeCreated>(fun (created: EmployeeCreated) -> state := {state.Value with Name = created.Name; Position = created.Position; Salary = created.Salary; DateHired = created.DateHired})
    do actor.Recover<EmployeeRenamed>(fun (renamed: EmployeeRenamed) -> state := {state.Value with Name = renamed.Name})
    do actor.Recover<SalaryChanged>(fun (changed: SalaryChanged) -> state := {state.Value with Salary = changed.Salary})

    do actor.Command<CreateEmployee>(fun (create: CreateEmployee) -> 
        let created = {EmployeeId = state.Value.Id; Name = create.Name; Position = create.Position; DateHired = DateTime.Now; Salary = create.Salary}
        actor.Persist(created, (fun created -> 
            state := {state.Value with Name = created.Name; Position = created.Position; Salary = created.Salary; DateHired = created.DateHired}
            context.Parent <! state.Value)))

    do actor.Command<RenameEmployee>(fun (rename: RenameEmployee) ->
        let renamed = {Name = rename.Name}
        actor.Persist(renamed, (fun renamed -> 
            state := {state.Value with Name = renamed.Name}
            context.Parent <! state.Value)))

    do actor.Command<ChangeSalary>(fun (change: ChangeSalary) ->
        let changed = {Salary = change.Salary}
        actor.Persist(changed, (fun changed -> 
            state := {state.Value with Salary = changed.Salary}
            context.Parent <! state.Value)))

    do actor.Command<SnapshotCommand>(fun _ -> actor.SaveSnapshot(state.Value))
    do actor.Command<SaveSnapshotSuccess>(fun (success: SaveSnapshotSuccess) -> 
        actor.DeleteMessages(success.Metadata.SequenceNr)
        actor.DeleteSnapshots(SnapshotSelectionCriteria(success.Metadata.SequenceNr, success.Metadata.Timestamp.AddMilliseconds(-1.0))))

    override __.PersistenceId = sprintf "employee-%A" employeeId
    static member Props id = 
        let toProps expr =
            let e = expr |> Linq.RuntimeHelpers.LeafExpressionConverter.QuotationToExpression
            let call = e :?> MethodCallExpression
            let lambda = call.Arguments.[0] :?> LambdaExpression
            Expression.Lambda<Func<EmployeeActor>>(lambda.Body)
        Props.Create(<@ fun () -> EmployeeActor(id) @> |> toProps)

type EmployeesActor () as actor =
    inherit ReceivePersistentActor()
    let context = ActorBase.Context
    let employees = Dictionary<Guid, Employee>()

    do actor.Recover<SnapshotOffer>(fun (offer: SnapshotOffer) -> 
        let employeeArray = offer.Snapshot |> unbox<Employee []>
        for employee in employeeArray do employees.Add(employee.Id, employee))

    do actor.Recover<Employee>(fun (employee: Employee) -> 
        let name = sprintf "employee-%A" employee.Id
        let childActor = context.ActorOf(EmployeeActor.Props employee.Id, name)
        employees.[employee.Id] <- employee)

    do actor.Command<Employee>(fun (employee: Employee) -> actor.Persist(employee, fun _ -> employees.[employee.Id] <- employee))
    do actor.Command<EmployeesCommand>(fun (command: EmployeesCommand) -> 
        match command with
        | GetEmployees -> context.Sender <! (employees.Values |> Seq.toList)
        | PrintEmployees -> for employee in employees.Values do printfn "%A" employee)

    do actor.Command<CreateEmployee>(fun (create: CreateEmployee) -> 
        let id = Guid.NewGuid()
        let name = sprintf "employee-%A" id
        let childActor = context.ActorOf(EmployeeActor.Props id, name)
        childActor <! create)

    do actor.Command<RenameEmployee>(fun (rename: RenameEmployee) -> 
        let name = sprintf "employee-%A" rename.Id
        let childActor = context.Child(name)
        childActor <! rename)

    do actor.Command<ChangeSalary>(fun (change: ChangeSalary) -> 
        let name = sprintf "employee-%A" change.Id
        let childActor = context.Child(name)
        childActor <! change)

    do actor.Command<SnapshotCommand>(fun _ -> 
        let children = context.GetChildren()
        for childActor in children do
            childActor <! TakeSnapshot
        actor.SaveSnapshot(employees.Values |> Seq.toArray))

    override __.PersistenceId = "all-employees"



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
                    server-name = "corpweiapd001"
                    write-batch-size = 4095
                    read-batch-size = 4095
                }
              }
              snapshot-store {
                plugin = "akka.persistence.snapshot-store.event-store"
                event-store {
                    class = "Akka.Persistence.EventStore.EventStoreSnapshotStore, CurryOn.Akka.Persistence.EventStore"
                    server-name = "corpweiapd001"
                    read-batch-size = 4095
                }
              }
            }
          }"""

let akka = System.create "company" <| Configuration.parse hocon

let employees = akka.ActorOf<EmployeesActor>("all-employees")

employees <! {Name = "Jim Gnomes"; Position = "Operator Overloader"; Salary = 32044.56M}
employees <! {Name = "Rob Hobbit"; Position = "Currier"; Salary = 54862.95M}
employees <! {Name = "Gerald Munk"; Position = "Lambda Invoker"; Salary = 48350.85M}

employees <! TakeSnapshot

let (allEmployees: Employee list) = employees <? GetEmployees |> Async.RunSynchronously

let jim = allEmployees |> List.find (fun e -> e.Name.StartsWith("Jim"))
employees <! {Id = jim.Id; Salary = 46044.56M}


let readJournal = new EventStoreReadJournal(akka :?> ExtendedActorSystem) 
let query = (readJournal :> Akka.Persistence.Query.ICurrentPersistenceIdsQuery)
query.CurrentPersistenceIds().RunForeach((fun id -> printfn "%s" id), Akka.Streams.ActorMaterializer.Create(akka))

let allQuery = (readJournal :> Akka.Persistence.Query.IAllPersistenceIdsQuery)
allQuery.AllPersistenceIds().RunForeach((fun id -> printf "%s" id), Akka.Streams.ActorMaterializer.Create(akka)) |> Task.runSynchronously

// Cleanup
let eventStoreTask = Configuration.parse hocon |> (fun config -> config.GetConfig("akka.persistence.journal.event-store")) |> Settings.Load |> EventStoreConnection.connect
let eventStore = eventStoreTask.Result
let credentials = UserCredentials("admin", "changeit")
eventStore.DeleteStreamAsync("all-employees", ExpectedVersion.Any |> int64, credentials) |> Task.ignoreSynchronously
eventStore.DeleteStreamAsync("snapshots-all-employees", ExpectedVersion.Any |> int64, credentials) |> Task.ignoreSynchronously
eventStore.DeleteStreamAsync("snapshot-all-employees-3", ExpectedVersion.Any |> int64, credentials) |> Task.ignoreSynchronously


// Test reply 
let settings = CatchUpSubscriptionSettings(CatchUpSubscriptionSettings.Default.MaxLiveQueueSize, 4095, false, true)

eventStore.SubscribeToStreamFrom("employee-9ff873e4-f991-4f61-9df4-a13986139019", 0L |> Nullable, settings, 
                                    (fun subscription event -> printfn "%A" event), 
                                    userCredentials = credentials)







open System.Net
let eventStore = EventStoreConnection.Create(IPEndPoint(IPAddress.Parse("10.100.1.35"), 1113))
let credentials = SystemData.UserCredentials("admin", "changeit")

eventStore.ConnectAsync() |> Task.ofUnit |> Task.runSynchronously

let rec readSlice startPosition ids =
    task {
        let! eventSlice = eventStore.ReadStreamEventsForwardAsync("$streams", startPosition, 4095, true, userCredentials = credentials)
        let newIds = 
            eventSlice.Events 
            |> Seq.map (fun resolved -> 
                printfn "%s" resolved.Event.EventStreamId
                if resolved.Event |> isNotNull
                then resolved.Event.EventStreamId
                else resolved.OriginalStreamId)
            |> Seq.filter (fun stream -> (stream.StartsWith("$") || stream.StartsWith("snapshots")) |> not )
            |> Seq.distinct
            |> Seq.fold (fun acc cur -> acc |> Set.add cur) ids
        if eventSlice.IsEndOfStream |> not
        then return! newIds |> readSlice eventSlice.NextEventNumber
        else return newIds
    }

let persistenceIds = Set.empty<string> |> readSlice 0L
for id in persistenceIds.Result do printfn "%s" id

