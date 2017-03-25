namespace CurryOn.Core

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

type IAggregate<'key, 'root when 'root :> IEntity<'key>> =
    inherit IEquatable<'key>
    abstract member Key: 'key
    abstract member Name: string<AggregateName>
    abstract member Tenant: string<Tenant>

type IMessage =
    abstract member Id: Guid
    abstract member CorrelationId: Guid
    abstract member MessageDate: DateTime

type ICommand = 
    inherit IMessage
    abstract member DateSent: DateTime option
    abstract member DateDelivered: DateTime option

type IEvent =
    inherit IMessage
    abstract member DatePublished: DateTime option

type IBus =
    abstract member SendCommand: ICommand -> AsyncResult<unit>
    abstract member PublishEvent: IEvent -> AsyncResult<unit>

