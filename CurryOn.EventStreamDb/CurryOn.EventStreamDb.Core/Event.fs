namespace CurryOn.EventStreamDb

open CurryOn.Common
open System

type IEventType =
    abstract member TypeName: string
    abstract member ClrType: Type option
    abstract member Version: int

type IEventMetadata =
    abstract member Type: IEventType
    abstract member Tags: string []
    abstract member UserData: byte []

type IEvent =
    inherit IEquatable<IEvent>
    inherit IComparable<IEvent>
    abstract member Id: Guid
    abstract member EventNumber: int64
    abstract member StreamId: string
    abstract member Timestamp: DateTime
    abstract member Metadata: IEventMetadata
    abstract member EventData: byte []


type PersistenceSerialization =
| Binary
| Text
| JsonBinary
| JsonText
| XmlBinary
| XmlText

[<CLIMutable>]
type PersistedEventType =
    {
        TypeName: string
        ClrType: string
        Version: int
    } interface IEventType with
        member this.TypeName = this.TypeName
        member this.ClrType = Types.findType this.TypeName |> Result.toOption
        member this.Version = this.Version

[<CLIMutable>]
type PersistedEventMetadata =
    {
        Type: PersistedEventType
        Tags: string []
        Serialization: PersistenceSerialization
        UserData: byte []
    } interface IEventMetadata with
        member this.Type = this.Type :> IEventType
        member this.Tags = this.Tags
        member this.UserData = this.UserData

[<CLIMutable>]
[<CustomEquality>]
[<CustomComparison>]
type PersistedEvent =
    {
        Id: Guid
        EventNumber: int64
        StreamPosition: int64
        AbsolutePosition: int64
        StreamId: string
        Timestamp: DateTime
        Metadata: PersistedEventMetadata
        EventData: byte []
    }
    override this.Equals (o: obj) =
        if o |> isNull then false
        else match o with
             | :? IEvent as e -> this.Id = e.Id
             | _ -> false
    override this.GetHashCode () = this.Id.GetHashCode()
    member this.CompareTo (o: obj) =
        if o |> isNull then 1
        else match o with
             | :? PersistedEvent as p ->
                let difference = this.StreamPosition - p.StreamPosition
                if difference > 0L then 1 elif difference < 0L then -1 else 0
             | :? IEvent as e -> 
                let difference = this.EventNumber - e.EventNumber
                if difference > 0L then 1 elif difference < 0L then -1 else 0
             | _ -> 1
    interface IEvent with
        member this.Id = this.Id
        member this.EventNumber = this.EventNumber
        member this.StreamId = this.StreamId
        member this.Timestamp = this.Timestamp
        member this.Metadata = this.Metadata :> IEventMetadata
        member this.EventData = this.EventData
        member this.Equals otherEvent = this.Equals otherEvent
        member this.CompareTo otherEvent = this.CompareTo otherEvent
        
