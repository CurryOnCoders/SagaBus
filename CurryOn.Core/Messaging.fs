namespace CurryOn.Core

open Akka.Routing
open CurryOn.Common
open System
open System.Text.RegularExpressions

[<Measure>] type Tenant         = static member New: string -> string<Tenant>          = (fun v -> {Value = v})
[<Measure>] type AggregateName  = static member New: string -> string<AggregateName>   = (fun v -> {Value = v})
[<Measure>] type Version        = static member New (value: #IComparable) = value |> Convert.ToInt32 |> LanguagePrimitives.Int32WithMeasure<Version>

type IValueObject =
    inherit IEquatable<IValueObject>

type IEntity<'id> =
    inherit IEquatable<'id>
    abstract member Id: 'id

type IAggregateIdentity<'key> =
    inherit IConsistentHashable
    abstract member Key: 'key
    abstract member Name: string<AggregateName>
    abstract member Tenant: string<Tenant>

type IAggregate<'key, 'root when 'root :> IEntity<'key>> =
    inherit IAggregateIdentity<'key>
    inherit IEquatable<'key>
    abstract member Apply : IEvent<'key> -> int<Version> -> IAggregate<'key, 'root>

and IMessage<'key> =
    inherit IAggregateIdentity<'key>
    abstract member Id: Guid
    abstract member CorrelationId: Guid
    abstract member MessageDate: DateTime

and ICommand<'key> = 
    inherit IMessage<'key>
    abstract member DateSent: DateTime option
    abstract member DateDelivered: DateTime option

and IEvent<'key> =
    inherit IMessage<'key>
    abstract member DatePublished: DateTime option

[<Measure>] 
type EndpointAddress = 
    static member New: string -> string<EndpointAddress> = (fun v -> {Value = v})

type IBusEndpoint =
    abstract member Address: string<EndpointAddress>

[<CustomEquality>]
[<CustomComparison>]
type MessageType = 
    { Name: string; ClrType: Type }
    static member FromType clrType = { ClrType = clrType; Name = clrType.Name }
    override this.Equals (other: obj) =
        match other with
        | null -> false
        | :? MessageType as messageType -> messageType.ClrType.Equals(this.ClrType)
        | :? Type as clrType -> this.ClrType.Equals(clrType)
        | :? string as name -> this.Name = name
        | _ -> false
    override this.GetHashCode () = this.Name.GetHashCode()
    interface IComparable with
        member this.CompareTo other =
            match other with
            | null -> 1
            | :? MessageType as messageType -> this.Name.CompareTo(messageType.Name)
            | :? Type as clrType -> this.ClrType.Name.CompareTo(clrType.Name)
            | :? string as name -> this.Name.CompareTo(name)
            | _ -> 1
    interface IComparable<MessageType> with
        member this.CompareTo messageType =
            this.Name.CompareTo(messageType.Name)
    interface IEquatable<Type> with
        member this.Equals clrType =
            this.ClrType.Equals(clrType)

type IBusRoute =
    abstract member MessageType: MessageType
    abstract member Endpoint: IBusEndpoint

type ICommandRouter =
    abstract member Register: MessageType -> IBusEndpoint -> AsyncResult
    abstract member Route: ICommand<_> -> AsyncResult

type ICommandReceiver =
    abstract member ReceiveCommands : unit -> IObservable<ICommand<_>>

/// The IEventHub is the persistence component on an IBusNode.
/// While there can be multiple Command & Event Receivers and many Comand Routers,
/// only one EventStore can be written to, so only one IEventHub can be used.
type IEventHub =
    abstract member PersistEvent: IEvent<_> -> AsyncResult

type IEventReceiver =
    abstract member ReceiveEvents : unit -> IObservable<IEvent<_>> 

type IBus =
    abstract member SendCommand: ICommand<_> -> AsyncResult
    abstract member PublishEvent: IEvent<_> -> AsyncResult

type IBusNode =
    inherit IBus
    abstract member CommandRouters: Map<MessageType, ICommandRouter>
    abstract member CommandReceivers: ICommandReceiver list
    abstract member EventReceivers: IEventReceiver list
    abstract member EventPublisher: IEventHub

type ISaga<'aggregate, 'key, 'root when 'aggregate :> IAggregate<'key,'root> and 'root :> IEntity<'key>> =
    abstract member SagaBus: IBus

