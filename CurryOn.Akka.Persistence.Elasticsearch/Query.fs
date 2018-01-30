namespace CurryOn.Akka

open Akka.Persistence.Query
open CurryOn.Elastic
open FSharp.Control
open Reactive.Streams

module internal PersistenceQuery =
    let inline toUpperBound offset =
        match offset with
        | Some i -> LessThanOrEqual i
        | None -> UnboundedUpper

    let inline toLowerBound offset =
        match offset with
        | Some i -> GreaterThanOrEqual i
        | None -> UnboundedLower

    let inline hitToEventEnvelope (hit: Hit<PersistedEvent>) =
        EventEnvelope(hit.Id.ToInt(), hit.Document.PersistenceId, hit.Document.SequenceNumber, hit.Document.Event |> Serialization.parseJson<obj>)

    let inline processHits (subscriber: ISubscriber<EventEnvelope>) (search: SearchResult<PersistedEvent>)  =
        search.Results.Hits
        |> List.map hitToEventEnvelope
        |> List.iter subscriber.OnNext

    let inline getCurrentPersistenceIds client =
        operation {
            let! distinctValues = client |> Search.distinct<PersistedEvent,string> <@ fun persistedEvent -> persistedEvent.PersistenceId @> None
            return! distinctValues.Aggregations |> List.map (fun value -> value.Key) |> Result.success
        }

    let inline getCurrentEvents client =
        operation {
            let sort = Some <| Sort.ascending <@ fun (persistedEvent: PersistedEvent) -> persistedEvent.SequenceNumber @>
            return! MatchAll None |> Dsl.execute<PersistedEvent> client None sort None None
        } 

    let inline getCurrentEventsByTag client tag lowOffset highOffset =
        operation {
            let fromOffset = lowOffset |> toLowerBound
            let toOffset = highOffset |> toUpperBound
            return! [Dsl.terms<PersistedEvent,string> <@ fun persistedEvent -> persistedEvent.Tags @> [tag]]
                    |> Dsl.bool [] [Dsl.range<PersistedEvent,int64> <@ fun persistedEvent -> persistedEvent.SequenceNumber @> fromOffset toOffset None] []
                    |> Dsl.execute<PersistedEvent> client None (Some <| Sort.ascending <@ fun persistedEvent -> persistedEvent.SequenceNumber @>) None None
        }

    let inline getCurrentEventsByPersistenceId client persistenceId min max =
        operation {
            let inline toBound opt = 
                match opt with
                | Some value -> Inclusive value
                | None -> Unbounded
            let fromSequence = min |> toBound
            let toSequence = max |> toBound
            return!
                Query.field<PersistedEvent,string> <@ fun persistedEvent -> persistedEvent.PersistenceId @> persistenceId
                |> Query.And (Query.range<PersistedEvent,int64> <@ fun persistedEvent -> persistedEvent.SequenceNumber @> fromSequence toSequence)
                |> Query.execute<PersistedEvent> client None (Some <| Sort.ascending <@ fun persistedEvent -> persistedEvent.SequenceNumber @>) None None
        } 
