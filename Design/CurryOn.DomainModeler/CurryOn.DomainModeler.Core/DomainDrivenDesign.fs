namespace CurryOn.DomainModeler

open FSharp.Quotations
open System

type TypeConstraint =
| IsA of Type
| HasMember of string * Type
| IsReferenceType
| IsValueType
| HasDefaultConstructor
| IsNullable
| SupportsComparison
| SupportsEquality 

type TypeParameter =
    {
        Name: string
        Constraints: TypeConstraint list
    }
    static member Default = {Name = "'a"; Constraints = []}

type Template =
| Basic
| Generic of TypeParameter list

type SnapshotPolicy =
| NeverTakeSnapshots
| SnapshotEveryEvent
| EventCountSinceLastSnapshot of int
| TimeSinceLastSnapshot of TimeSpan
with static member Default = EventCountSinceLastSnapshot 10

type LifecyclePolicy =
| AlwaysAlive
| UncoditionalTimeToLive of TimeSpan
| IdleTimeToLive of TimeSpan
| ShutdownWhenInboxIsEmpty
with static member Default = IdleTimeToLive <| TimeSpan.FromMinutes(10.0)

type Policy =
| SnapshotPolicy of SnapshotPolicy
| LifecyclePolicy of LifecyclePolicy

type Field =
    {
        Name: string
        Type: Type
    }
    static member Default = {Name = String.Empty; Type = typeof<obj>}

type Member =
| Field of Field
| Identity of Field
| Property of Expr

type ValueObject =
    {
        Name: string
        Template: Template
        Fields: Field list
    }
    static member Default = {Name = String.Empty; Template = Basic; Fields = []}

type Entity =
    {
        Name: string
        Template: Template
        Members: Member list
    }
    static member Default = {Name = String.Empty; Template = Basic; Members = []}

type Command =
    {
        Name: string
        AggregateName: string
        AggregateKey: string
        Members: Member list
    }
    static member Default = {Name = String.Empty; AggregateName = String.Empty; AggregateKey = String.Empty; Members = []}

type EventType =
| Initial
| StateChange of int64
| ErrorEvent of exn
| CompensatingEvent
with static member Default = Initial

type Event =
    {
        Name: string
        AggregateName: string
        AggregateKey: string
        EventType: EventType
        Members: Member list
    }
    static member Default = {Name = String.Empty; AggregateName = String.Empty; AggregateKey = String.Empty; EventType = EventType.Default; Members = []}

type Message =
| DomainCommand of Command
| DomainEvent of Event
with member message.IsEvent = 
        match message with
        | DomainEvent _ -> true
        | _ -> false
     member message.IsCommand =
        match message with
        | DomainCommand _ -> true
        | _ -> false

type Element =
| ValueObject of ValueObject
| Entity of Entity

type Cardinality =
| OneToOne
| OneToOneOrNone
| OneToMany
| ManyToMany
| OneOrNoneToMany
with static member Default = OneToOneOrNone

type Relationship =
    {
        Cardinality: Cardinality
        RelatedAggregate: Aggregate
    }
    with static member Default = {Cardinality = Cardinality.Default; RelatedAggregate = Aggregate.Default}

and Aggregate =
    {
        Name: string
        Template: Template
        AggregateRoot: Entity
        Elements: Element list
        DirectMembers: Member list
        Behaviors: Behavior list
        Messages: Message list
        Relationships: Relationship list
        Policies: Policy list
    }
    member aggregate.Commands = aggregate.Messages |> List.filter (fun message -> message.IsCommand)
    member aggregate.Events = aggregate.Messages |> List.filter (fun message -> message.IsEvent)
    static member Default = {Name = String.Empty; 
                             Template = Basic; 
                             AggregateRoot = Entity.Default; 
                             Elements = []; 
                             DirectMembers = []; 
                             Behaviors = [];
                             Messages = [];
                             Relationships = []; 
                             Policies = [SnapshotPolicy.Default |> SnapshotPolicy; LifecyclePolicy.Default |> LifecyclePolicy]
                            }

and ObjectType =
| Message of Message
| ValueObject of ValueObject
| Entity of Entity
| Aggregate of Aggregate
| BasicType of Type
with static member Default = Message <| DomainCommand Command.Default

and Parameter =
    {
        Name: string
        Type: ObjectType
    }
    static member Default = {Name = String.Empty; Type = ObjectType.Default}
    
and Behavior =
    {
        Name: string
        InputParameters: Parameter list
        ReturnType: ObjectType
        EventsRaised: Event list
    }
    static member Default = {Name = String.Empty; InputParameters = []; ReturnType = ObjectType.Default; EventsRaised = []}

type DomainService =
    {
        Name: string
        Behaviors: Behavior list
    }
    static member Default = {Name = String.Empty; Behaviors= []}

type BoundedContext =
    {
        Name: string
        Namespace: string
        Aggregates: Aggregate list
    }
    static member Default = {Name = String.Empty; Namespace = String.Empty; Aggregates = []}