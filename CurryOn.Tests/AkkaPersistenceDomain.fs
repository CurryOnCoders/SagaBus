namespace CurryOn.Tests

open Akka.Actor
open Akka.FSharp
open Akka.Persistence
open Akka.Routing
open System
open System.Collections.Generic
open System.Diagnostics
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

type StateChange<'state> =
| Apply of 'state
| Recover of 'state
with member this.State =
        match this with
        | Apply state -> state
        | Recover state -> state

type EmployeeActor (employeeId) as actor =
    inherit ReceivePersistentActor ()
    let context = ActorBase.Context
    let state = ref <| Employee.Create employeeId

    let updateState (stateChange: StateChange<Employee>) =
        state := stateChange.State
        match stateChange with
        | Apply _ -> context.Parent <! state.Value
        | _ -> ()

    let applySnapshot snapshot = snapshot |> unbox |> Recover |> updateState

    let applyEvent (stateChange: StateChange<_>) application = 
        let newState = application state.Value stateChange.State
        updateState <| 
            match stateChange with
            | Apply _ -> Apply newState
            | Recover _ -> Recover newState

    let applyCreated (event: EmployeeCreated StateChange) = 
        applyEvent event <| fun state created -> {state with Name = created.Name; Position = created.Position; Salary = created.Salary; DateHired = created.DateHired}
            
    let applyRenamed (event: EmployeeRenamed StateChange) = 
        applyEvent event <| fun state renamed -> {state with Name = renamed.Name}

    let applySalaryChanged (event: SalaryChanged StateChange) = 
        applyEvent event <| fun state changed -> {state with Salary = changed.Salary}

    do actor.Recover<SnapshotOffer>(fun (offer: SnapshotOffer) -> applySnapshot offer.Snapshot)
    do actor.Recover<EmployeeCreated>(fun event -> applyCreated <| Recover event)
    do actor.Recover<EmployeeRenamed>(fun event -> applyRenamed <| Recover event)
    do actor.Recover<SalaryChanged>(fun event -> applySalaryChanged <| Recover event)

    do actor.Command<CreateEmployee>(fun (create: CreateEmployee) -> 
        let created = {EmployeeId = state.Value.Id; Name = create.Name; Position = create.Position; DateHired = DateTime.Now; Salary = create.Salary}
        actor.Persist(created, (fun event -> applyCreated <| Apply event)))

    do actor.Command<RenameEmployee>(fun (rename: RenameEmployee) ->
        let renamed = {Name = rename.Name}
        actor.Persist(renamed, (fun event -> applyRenamed <| Apply event)))

    do actor.Command<ChangeSalary>(fun (change: ChangeSalary) ->
        let changed = {Salary = change.Salary}
        actor.Persist(changed, (fun event -> applySalaryChanged <| Apply event)))

    do actor.Command<SnapshotCommand>(fun _ -> actor.SaveSnapshot(state.Value))
    do actor.Command<SaveSnapshotSuccess>(fun (success: SaveSnapshotSuccess) -> 
        actor.DeleteMessages(success.Metadata.SequenceNr)
        actor.DeleteSnapshots(SnapshotSelectionCriteria(success.Metadata.SequenceNr, success.Metadata.Timestamp.AddMilliseconds(-1.0))))

    override __.PersistenceId = sprintf "employee-%A" employeeId
    override __.PostStop () = Debug.WriteLine(sprintf "Employee Actor %s Stopped" state.Value.Name)
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

    let updateEmployee (employee: Employee) =  employees.[employee.Id] <- employee

    do actor.Recover<SnapshotOffer>(fun (offer: SnapshotOffer) -> 
        let employeeArray = offer.Snapshot |> unbox<Employee []>
        for employee in employeeArray do 
            let name = sprintf "employee-%A" employee.Id
            let childActor = context.ActorOf(EmployeeActor.Props employee.Id)
            updateEmployee employee)

    do actor.Recover<Employee>(fun (employee: Employee) -> 
        let name = sprintf "employee-%A" employee.Id
        let childActor = context.ActorOf(EmployeeActor.Props employee.Id)
        updateEmployee employee)

    do actor.Command<Employee>(fun (employee: Employee) -> 
        if not <| (employees.ContainsKey employee.Id && employees.[employee.Id] = employee)
        then actor.Persist(employee, Action<Employee>(updateEmployee)))

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
    override __.PostStop () = Debug.WriteLine("All-Employees Actor Stopped")


