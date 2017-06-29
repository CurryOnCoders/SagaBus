namespace CurryOn.Core

open Akka.Routing
open CurryOn.Common
open System
open System.Threading.Tasks

type SnapshotPolicy =
| EveryEvent
| EventCount of int64
| TimeBased of TimeSpan

type IAggregateIdentity =
    inherit IConsistentHashable
    abstract member AggregateName: string
    abstract member AggregateKey: string    
    abstract member Tenant: string option

type IMessageHeader =
    inherit IAggregateIdentity
    abstract member MessageId: Guid
    abstract member CorrelationId: Guid
    abstract member MessageDate: DateTime

type IMessageBody = interface end

type ICommand = 
    inherit IMessageBody

type IEvent =
    inherit IMessageBody

type IMessage =
    abstract member Header: IMessageHeader
    abstract member Body: IMessageBody

type IAggregate =
    inherit IAggregateIdentity
    abstract member SnapshotPolicy: SnapshotPolicy
    abstract member LastEventNumber: int64
    abstract member Apply : IEvent -> int64 -> IAggregate

type IBusEndpoint =
    abstract member Address: string

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
    abstract member Register: MessageType -> IBusEndpoint -> Task<unit>
    abstract member Route: ICommand -> Task<unit>

type ICommandReceiver =
    abstract member ReceiveCommands : unit -> Task<IObservable<ICommand>>

/// The IEventHub is the persistence component on an IBusNode.
/// While there can be multiple Command & Event Receivers and many Comand Routers,
/// only one EventStore can be written to, so only one IEventHub can be used.
type IEventHub =
    abstract member PersistEvent: IEvent<_> -> Task<unit>

type IEventReceiver =
    abstract member ReceiveEvents : unit -> Task<IObservable<IEvent>>
    
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
    abstract member SendCommand: ICommand -> Task<unit>
    abstract member PublishEvent: IEvent -> Task<unit>
    abstract member Subscribe: ISubscription -> Task<unit>

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
    abstract member Connect: unit -> Task<'connection>

type IComponentWithCredentials<'credentials> =
    inherit IComponent
    abstract member GetCredentials: unit -> Result<'credentials>

type IMessageProcessingAgent =
    inherit IComponent
    abstract member ProcessMessage: IMessage -> Task<unit>

type SerializationFormat = 
    | Xml 
    | Json
    | Binary
    | Bson
    | Wcf
    | Positional
    | Delimited
    | Compressed of SerializationFormat   
    | Detect
    with
        member fmt.UnderlyingFormat =
            match fmt with
            | Xml -> Xml
            | Json -> Json
            | Binary -> Binary
            | Bson -> Bson
            | Wcf -> Wcf
            | Positional -> Positional
            | Delimited -> Delimited
            | Compressed format ->
                match format with
                | Xml -> Xml
                | Json -> Json
                | Binary -> Binary
                | Bson -> Bson
                | Wcf -> Wcf
                | Positional -> Positional
                | Delimited -> Delimited
                | Detect -> Detect
                | Compressed _ -> failwith "Only one layer of comrpession can be used for serialization"
            | Detect -> Detect


type IMessageMetadata =
    abstract member CorrelationId: Guid
    abstract member MessageId: Guid
    abstract member MessageDate: DateTime
    abstract member MessageName: string
    abstract member MessageFormat: SerializationFormat
    abstract member AggregateName: string
    abstract member AggregateKey: string
    abstract member Tenant: string