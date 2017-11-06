namespace CurryOn.EventStreamDb

open System

[<CLIMutable>]
type WriteEvent<'event> =
    {
        Id: Guid
        Stream: string
        EventData: 'event
        EventVersion: int option
        Timestamp: DateTime
        Tags: string []
    }

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