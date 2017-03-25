namespace CurryOn.Core

open Akka.Routing
open CurryOn.Common
open System

[<Measure>] type Tenant        = static member New: string -> string<Tenant>         = (fun v -> {Value = v})
[<Measure>] type AggregateName = static member New: string -> string<AggregateName>  = (fun v -> {Value = v})
[<Measure>] type Version       = static member New (value: #IComparable) = value |> Convert.ToInt32 |> LanguagePrimitives.Int32WithMeasure<Version>

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
    abstract member Apply : IEvent<'key> -> IAggregate<'key, 'root>

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

type IBus =
    abstract member SendCommand: ICommand<_> -> AsyncResult<unit>
    abstract member PublishEvent: IEvent<_> -> AsyncResult<unit>

