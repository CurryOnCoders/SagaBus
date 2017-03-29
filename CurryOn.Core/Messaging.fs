namespace CurryOn.Core

open Akka.Routing
open CurryOn.Common
open System
open System.Threading.Tasks

type IValueObject =
    inherit IEquatable<IValueObject>

type IEntity =
    abstract member Id: string<EntityId>

type IAggregateIdentity =
    inherit IConsistentHashable
    abstract member Key: string<AggregateKey>
    abstract member Name: string<AggregateName>
    abstract member Tenant: string<Tenant> option

type IAggregate =
    inherit IAggregateIdentity
    abstract member Root: IEntity
    abstract member LastEvent: int<version>
    abstract member Apply : IEvent -> int<version> -> IAggregate

and IMessage =
    inherit IAggregateIdentity
    abstract member MessageId: Guid
    abstract member CorrelationId: Guid
    abstract member MessageDate: DateTime

and ICommand = 
    inherit IMessage
    abstract member DateSent: DateTime option
    abstract member DateProcessed: DateTime option

and IEvent =
    inherit IMessage
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
    abstract member Route: ICommand -> AsyncResult

type ICommandReceiver =
    abstract member ReceiveCommands : unit -> AsyncResult<IObservable<ICommand>>

/// The IEventHub is the persistence component on an IBusNode.
/// While there can be multiple Command & Event Receivers and many Comand Routers,
/// only one EventStore can be written to, so only one IEventHub can be used.
type IEventHub =
    abstract member PersistEvent: IEvent<_> -> AsyncResult

type IEventReceiver =
    abstract member ReceiveEvents : unit -> AsyncResult<IObservable<IEvent>>
    
type SubscriptionType =
| Persistent
| Volatile

type SubscriptionMode =
| FromCurrent
| FromBeginning
| FromIndex of int64

type ISubscription =
    abstract member Name: string
    abstract member Type: SubscriptionType
    abstract member Mode: SubscriptionMode
    abstract member HandleEvent: IEvent -> Result

type IBus =
    abstract member SendCommand: ICommand -> AsyncResult
    abstract member PublishEvent: IEvent -> AsyncResult
    abstract member Subscribe: ISubscription -> AsyncResult

type IBusClient =
    abstract member SendCommand: ICommand -> Task<Result>
    abstract member PublishEvent: IEvent -> Task<Result>
    abstract member Subscribe: ISubscription -> Task<Result>

type IBusNode =
    inherit IBus
    abstract member CommandRouters: Map<MessageType, ICommandRouter>
    abstract member CommandReceivers: ICommandReceiver list
    abstract member EventReceivers: IEventReceiver list
    abstract member EventPublisher: IEventHub

type ISaga<'aggregate when 'aggregate :> IAggregate> =
    abstract member SagaBus: IBus

type IComponentWithConnection<'connection> =
    inherit IComponent
    abstract member Connect: unit -> AsyncResult<'connection>

type IComponentWithCredentials<'credentials> =
    inherit IComponent
    abstract member GetCredentials: unit -> Result<'credentials>

type SerializationFormat = 
    | Xml 
    | Json
    | Wcf
    | Positional
    | Compressed of SerializationFormat   
    | Detect
    | Unsupported
    with
        member fmt.UnderlyingFormat =
            match fmt with
            | Xml -> Xml
            | Json -> Json
            | Wcf -> Wcf
            | Positional -> Positional
            | Compressed format ->
                match format with
                | Xml -> Xml
                | Json -> Json
                | Wcf -> Wcf
                | Positional -> Positional
                | Detect -> Detect
                | _-> Unsupported
            | Detect -> Detect
            | Unsupported -> Unsupported


type IMessageMetadata =
    abstract member CorrelationId: Guid
    abstract member MessageId: Guid
    abstract member MessageDate: DateTime
    abstract member MessageName: string<TypeName>
    abstract member MessageFormat: SerializationFormat
    abstract member AggregateName: string<AggregateName>
    abstract member AggregateKey: string<AggregateKey>
    abstract member Tenant: string<Tenant>