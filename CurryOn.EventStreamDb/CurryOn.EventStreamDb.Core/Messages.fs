namespace CurryOn.EventStreamDb

open System

type IWriteEvent =
    abstract member Id: Guid
    abstract member Stream: string
    abstract member EventData: obj
    abstract member EventVersion: int option
    abstract member Timestamp: DateTime
    abstract member Tags: string []


[<CLIMutable>]
type WriteEvent<'event> =
    {
        Id: Guid
        Stream: string
        EventData: 'event
        EventVersion: int option
        Timestamp: DateTime
        Tags: string []
    } interface IWriteEvent with
        member this.Id = this.Id
        member this.Stream = this.Stream
        member this.EventData = this.EventData |> box
        member this.EventVersion = this.EventVersion
        member this.Timestamp = this.Timestamp
        member this.Tags = this.Tags

[<CLIMutable>]
type WriteEventSucceeded =
    {
        Id: Guid
        Stream: string
        EventNumber: int64
    }

[<CLIMutable>]
type WriteEventFailed =
    {
        Id: Guid
        Stream: string
        Error: string
    }