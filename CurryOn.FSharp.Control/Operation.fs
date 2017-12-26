namespace FSharp.Control

open FSharp.Reflection
open System
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks

/// Represents the successful of an operation that also yields events,
/// such as warnings, informational messages, or other domain events
[<Struct>]
type SuccessfulResultWithEvents<'result,'event> =
    {
        Value: 'result
        Events: 'event list
    }

/// Represents the successful result of a computation,
/// which can be either a value or a value with a list of events
[<Struct>]
type SuccessfulResult<'result,'event> =
    | Value of ResultValue: 'result
    | WithEvents of ResultWithEvents: SuccessfulResultWithEvents<'result,'event>
    member this.Result =
        match this with
        | Value value -> value
        | WithEvents withEvents -> withEvents.Value
    member this.Events =
        match this with
        | Value _ -> []
        | WithEvents withEvents -> withEvents.Events    

/// Represents the result of a completed operation,
/// which can be either a Success or a Failure
[<Struct>]
type OperationResult<'result,'event> =
    | Success of Result: SuccessfulResult<'result,'event>
    | Failure of ErrorList: 'event list
    member this.Events =
        match this with
        | Success successfulResult -> successfulResult.Events
        | Failure events -> events
    override this.ToString () =
        match this with
        | Success successfulResult -> sprintf "OK: %A - %s" successfulResult.Result (String.Join(Environment.NewLine, successfulResult.Events |> Seq.map (fun x -> x.ToString())))
        | Failure errors -> sprintf "Error: %s" (String.Join(Environment.NewLine, errors |> Seq.map (fun x -> x.ToString())))    

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Result =
    type UnionCaseInfo with member this.Fields = this.GetFields()

    /// Creates a successful OperationResult with the given value
    let success<'result, 'event> : 'result -> OperationResult<'result,'event> = fun value -> value |> Value |> Success

    /// Creates a successful OperationResult with the given value and events
    let successWithEvents value events = {Value = value; Events = events} |> WithEvents |> Success

    /// Creates a failed OperationResult with the given error events
    let failure<'result,'event> : 'event list -> OperationResult<'result,'event> = fun events -> events |> Failure

    /// Unwraps an AggregateException if there is only 1 inner exception
    let unwrapAggregateException (aggregate: AggregateException) =
        if aggregate.InnerExceptions.Count = 1
        then aggregate.InnerExceptions |> Seq.head
        else aggregate :> exn

    /// Converts a System.Exception to an instance of the 'event type and then creates a Failure OperationResult with that event
    let inline convertExceptionToResult<'result,'event> (except: exn) =
        let ex = 
            match except with
            | :? AggregateException as agg -> unwrapAggregateException agg
            | _ -> except
        let union = FSharpType.GetUnionCases(typeof<OperationResult<'result, 'event>>)
        if typeof<'event>.IsAssignableFrom(typeof<exn>)
        then FSharpValue.MakeUnion(union.[1], [|[ex] |> box|]) |> unbox<OperationResult<'result, 'event>>
        elif FSharpType.IsUnion typeof<'event>
        then let cases = FSharpType.GetUnionCases(typeof<'event>)
             match cases |> Seq.tryFind (fun case -> case.Fields.Length = 1 && case.Fields.[0].PropertyType.IsAssignableFrom(typeof<exn>)) with
             | Some case -> FSharpValue.MakeUnion(union.[1], [|[FSharpValue.MakeUnion(case, [|ex |> box|]) |> unbox<'event>] |> box|]) |> unbox<OperationResult<'result, 'event>>
             | None -> failwithf "No Union Case of Event Type %s Supports Construction from an Unhandled Exception: \r\n%O" typeof<'event>.Name ex
        else failwithf "Unable To Construct a Failure of type %s from Unhandled Exception: \r\n%O" typeof<'event>.Name ex

    /// Merge the results and the domain events of two OperationResults
    let inline merge (result1: OperationResult<unit,'event>) (result2: OperationResult<'result,'event>) =
        match result1 with
        | Success successfulResult1 ->
            match successfulResult1 with
            | Value value1 -> 
                match result2 with
                | Success successfulResult2 -> 
                    match successfulResult2 with
                    | Value value2 -> success (value1,value2)
                    | WithEvents withEvents2 -> successWithEvents (value1,withEvents2.Value) withEvents2.Events
                | Failure events2 -> Failure events2
            | WithEvents withEvents1 ->
                match result2 with
                | Success successfulResult2 -> 
                    match successfulResult2 with
                    | Value value2 -> successWithEvents (withEvents1.Value,value2) withEvents1.Events
                    | WithEvents withEvents2 -> successWithEvents (withEvents1.Value,withEvents2.Value) (withEvents1.Events@withEvents2.Events)
                | Failure events2 -> Failure (withEvents1.Events@events2)
        | Failure events1 -> 
            match result2 with
            | Success successfulResult2 ->
                match successfulResult2 with
                | Value value -> Failure events1
                | WithEvents withEvents2 -> Failure (events1@withEvents2.Events)
            | Failure events2 -> Failure (events1@events2)   

    /// Merges the given results into the existing OperationResult
    let inline mergeEvents result events =
        match result with
        | Success successfulResult ->
            match successfulResult with
            | Value value -> successWithEvents value events
            | WithEvents withEvents -> successWithEvents withEvents.Value (events@withEvents.Events)
        | Failure errorEvents ->
            failure (events@errorEvents)

/// Represents an incomplete operation that is currently executing
[<Struct>]
type InProcessOperation<'result,'event> = 
    {
        Task: Task<'result>
        EventsSoFar: 'event list
    }

/// Represents an Operation composed of one or more steps,
/// which may be already completed, in-process, or cancelled
and [<Struct>] Operation<'result,'event> =
    | Completed of Result: OperationResult<'result,'event>
    | InProcess of IncompleteOperation: InProcessOperation<'result,'event>
    | Cancelled of EventsSoFar: 'event list
    member this.Events =
        match this with
        | Completed result -> result.Events
        | InProcess inProcess -> inProcess.EventsSoFar
        | Cancelled events -> events

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Operation =
    /// Creates a completed Operation with a successful OperationResult of the given value
    let success<'result, 'event> : 'result -> Operation<'result,'event> = fun value -> value |> Result.success<'result,'event> |> Completed

    /// Creates a completed Operation with a successful OperationResult of the given value and events
    let successWithEvents value events = Completed <| Result.successWithEvents value events

    /// Creates a failed Operation with the given errors in its OperationResult
    let failure<'result,'event> : 'event list -> Operation<'result,'event> = fun events -> events |> Result.failure<'result,'event> |> Completed

    /// Synchronously returns the operation of a result, waiting for it to complete if necessary
    let wait operation =
        match operation with
        | Completed result -> result
        | InProcess inProcess -> inProcess.Task.ContinueWith(fun (t: Task<_>) -> 
            if t.IsFaulted
            then Result.convertExceptionToResult t.Exception
            elif t.IsCanceled
            then Result.convertExceptionToResult <| OperationCanceledException()
            else t.Result |> Result.success).Result
        | Cancelled events -> Failure events

    /// Merges the given events into the existing operation
    let mergeEvents operation events =
        match operation with
        | Completed result -> Completed <| Result.mergeEvents result events
        | InProcess inProcess -> {inProcess with EventsSoFar = (events@inProcess.EventsSoFar)} |> InProcess
        | Cancelled eventsSoFar -> events @ eventsSoFar |> Cancelled