namespace FSharp.Control

open FSharp.Reflection
open System
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module OperationBuilder =
    /// Represents the completion notification and continuation for an asynchronous operation
    [<Struct>]
    type AsyncOperation<'result,'event> =
        {
            Completion: ICriticalNotifyCompletion
            Continuation: unit -> OperationStep<'result,'event>
        }

    /// Represents the state of an operation, which can either be 
    /// asynchronously waiting for something with a continuation,
    /// or have completed with a return value and events.
    and [<Struct>] OperationStep<'result,'event> =
        | Await of Async: AsyncOperation<'result,'event>
        | Return of Operation: OperationResult<'result,'event>
        | ReturnFrom of From: 'result Task
        | Lazy of Deferred: EventingLazy<OperationStep<'result,'event>>

    /// Provides the mechanism for running an `OperationStep<'result,'event>` as a task returning a continuation task.
    and OperationStepStateMachine<'result,'event>(firstStep: OperationStep<'result,'event>) as this =
        let methodBuilder = AsyncTaskMethodBuilder<'result Task>()
        let continuation = ref <| fun () -> firstStep
        let rec processStep step =
            match step with
            | Return r ->
                match r with
                | Success successfulResult -> methodBuilder.SetResult(Task.FromResult(successfulResult.Result))
                | Failure _ as error -> methodBuilder.SetException(exn <| error.ToString())
                None
            | ReturnFrom t ->
                methodBuilder.SetResult(t)
                None
            | Await async ->
                continuation := async.Continuation
                Some async.Completion
            | Lazy deferred ->
                let task = deferred.Evaluated  |> Async.AwaitEvent |> Async.StartAsTask
                let awaiter = task.GetAwaiter()
                continuation := fun () -> deferred.Value
                Some (awaiter :> ICriticalNotifyCompletion)
        let nextAwaitable() =
            try continuation.Value() |> processStep
            with | ex ->
                methodBuilder.SetException(ex)
                None
        let self = ref this

        /// Start execution as a `Task<Task<'a>>`.
        member __.Run() =
            methodBuilder.Start(self)
            methodBuilder.Task
    
        interface IAsyncStateMachine with
            /// Proceed to one of three states: result, failure, or awaiting.
            /// If awaiting, MoveNext() will be called again when the awaitable completes.
            member __.MoveNext() =
                let await = nextAwaitable()
                match await with
                | Some awaitable ->
                    let awaitableCell = ref awaitable
                    methodBuilder.AwaitUnsafeOnCompleted(awaitableCell, self)    
                | None -> ()
            member __.SetStateMachine(_) = () // Doesn't apply 

    /// Used to represent no-ops like the implicit empty "else" branch of an "if" expression.
    let inline zero () = Return <| Result.success()

    /// Used to return a value.
    let inline ret x = Return <| Success x

    /// Used to return a value.
    let inline retEx (x : 'a) = Return <| Result.success<'a,exn> x

    /// Binding function for operation results
    let inline bindResult (result: OperationResult<'a,'event>) (cont: 'a -> OperationStep<'b,'event>) =
        match result with
        | Success successfulResult -> cont successfulResult.Result
        | Failure errors -> Return <| Failure errors

    /// Primary binding function for operations
    let rec bind (op: Operation<'a,'event>) (cont: 'a -> OperationStep<'b,'event>) =
        match op with
        | Completed result -> 
            bindResult result cont
        | InProcess inProcess ->
            let awt = inProcess.Task.ConfigureAwait(false).GetAwaiter()
            if awt.IsCompleted 
            then cont(awt.GetResult())  // Proceed to the next step based on the result we already have.                
            else Await {Completion = awt; Continuation = (fun () -> cont(awt.GetResult()))} // Await and continue later when a result is available.
        | Deferred deferred ->
            bind deferred.Value cont
        | Cancelled events -> 
            Return <| Failure events

    type Binder<'result,'event> =
        static member inline private GenericAwait< ^awaitable, ^awaiter, ^input
                                                    when ^awaitable : (member GetAwaiter: unit -> ^awaiter)
                                                    and  ^awaiter   :> ICriticalNotifyCompletion 
                                                    and  ^awaiter   : (member get_IsCompleted: unit -> bool)
                                                    and  ^awaiter   : (member GetResult: unit -> ^input) >
            (awaitable: ^awaitable, continuation: ^input -> OperationStep<'result,'event>) : OperationStep<'result,'event> =
                let awaiter = (^awaitable: (member GetAwaiter: unit -> ^awaiter) (awaitable)) // get an awaiter from the awaitable
                if (^awaiter: (member get_IsCompleted: unit -> bool) (awaiter)) 
                then continuation (^awaiter: (member GetResult: unit -> ^input) (awaiter)) // shortcut to continue immediately
                else Await {Completion = awaiter; Continuation = (fun () -> continuation (^awaiter: (member GetResult: unit -> ^input) (awaiter)))}

        static member inline GenericAwaitNoContext< ^taskLike, ^awaitable, ^awaiter, ^input
                                                     when ^taskLike : (member ConfigureAwait: bool -> ^awaitable)
                                                     and ^awaitable : (member GetAwaiter: unit -> ^awaiter)
                                                     and ^awaiter   :> ICriticalNotifyCompletion 
                                                     and ^awaiter   : (member get_IsCompleted: unit -> bool)
                                                     and ^awaiter   : (member GetResult: unit -> ^input) >
            (taskLike: ^taskLike, continuation: ^input -> OperationStep<'result,'event>) : OperationStep<'result,'event> =
                let awaitable = (^taskLike: (member ConfigureAwait: bool -> ^awaitable) (taskLike, false))
                Binder<'result,'event>.GenericAwait(awaitable, continuation)

        static member inline GenericAwaitCaptureContext (awaitable, continuation) = 
            Binder<'result,'event>.GenericAwait(awaitable, continuation)

    /// Special binding for Task<'a> that captures the current context for running the continuation. 
    /// Have to write this out by hand to avoid confusing the compiler
    /// trying to decide between satisfying the constraints with `Task` or `Task<'a>`.
    let inline bindTaskCaptureContext (task : 'a Task) (continuation : 'a -> OperationStep<'b,'event>) =
        let awaiter = task.GetAwaiter()
        if awaiter.IsCompleted 
        then let result = awaiter.GetResult()
             continuation result  // Proceed to the next step based on the result we already have.
        else Await {Completion = awaiter; Continuation = (fun () -> // Await and continue later when a result is available.
                let result = awaiter.GetResult() 
                continuation result)}  

    /// Special binding for Task<'a> without capturing the context.
    /// Have to write this out by hand to avoid confusing the compiler thinking our built-in bind method
    /// defined on the builder has fancy generic constraints on inp and out parameters.
    let inline bindTaskNoContext (task : 'a Task) (continuation : 'a -> OperationStep<'b,'event>) =
        let awaiter = task.ConfigureAwait(false).GetAwaiter()
        if awaiter.IsCompleted 
        then let result = awaiter.GetResult() 
             continuation result // Proceed to the next step based on the result we already have.
        else Await {Completion = awaiter; Continuation = (fun () ->  // Await and continue later when a result is available.
                let result = awaiter.GetResult()
                continuation result)}

    /// Special case for binding F# Async<'a> without having to always call Async.StartAsTask in the Operation computation
    let inline bindAsync (asyncVal: 'a Async) (continuation: 'a -> OperationStep<'b, 'event>) =
        bindTaskNoContext (asyncVal |> Async.StartAsTask) continuation

    /// Chains together a step with its following step.
    /// Note that this requires that the first step has no result.
    /// This prevents constructs like `operation { return 1; return 2; }`.
    let rec combine (step : OperationStep<unit,'event>) (continuation : unit -> OperationStep<'result,'event>) =
        match step with
        | Return _ -> continuation()
        | ReturnFrom t ->
            Await {Completion = t.GetAwaiter(); Continuation = continuation}
        | Await async ->
            Await {Completion = async.Completion; Continuation = (fun () -> combine (async.Continuation()) continuation)}
        | Lazy deferred ->
            lazy(combine deferred.Value continuation) |> EventingLazy |> Lazy

    /// Builds a step that executes the body while the condition predicate is true.
    let rec whileLoop (cond : unit -> bool) (body : unit -> OperationStep<unit,'event>) =
        if cond()  
        then let rec repeat () = // Create a self-referencing closure to test whether to repeat the loop on future iterations.
                if cond() then
                    let body = body()
                    match body with
                    | Return _ -> repeat()
                    | ReturnFrom t -> Await {Completion = t.GetAwaiter(); Continuation = repeat}
                    | Await async -> Await {async with Continuation = (fun () -> combine (async.Continuation()) repeat)}
                    | Lazy deferred -> lazy(whileLoop cond (fun () -> deferred.Value)) |> EventingLazy |> Lazy
                else zero ()
             // Run the body the first time and chain it to the repeat logic.
             combine (body()) repeat
        else zero ()

    /// Wraps a step in a try/with. This catches exceptions both in the evaluation of the function
    /// to retrieve the step, and in the continuation of the step (if any).
    let rec tryWith(step : unit -> OperationStep<'result,'event>) (catch : exn -> OperationStep<'result,'event>) =
        try match step() with
            | Return _ as i -> i
            | ReturnFrom t ->
                let awaitable = t.GetAwaiter()
                Await {Completion = awaitable; Continuation = (fun () ->
                    try awaitable.GetResult() |> Result.success |> Return
                    with | exn -> catch exn)}
            | Await async -> Await {async with Continuation = (fun () -> tryWith async.Continuation catch)}
            | Lazy deferred -> lazy(tryWith (fun () -> deferred.Value) catch) |> EventingLazy |> Lazy
        with | exn -> catch exn

    /// Wraps a step in a try/finally. This catches exceptions both in the evaluation of the function
    /// to retrieve the step, and in the continuation of the step (if any).
    let rec tryFinally (step : unit -> OperationStep<'result,'event>) fin =
        let step =
            // Important point: we use a try/with, not a try/finally, to implement tryFinally.
            // The reason for this is that if we're just building a continuation, we definitely *shouldn't*
            // execute the `fin()` part yet -- the actual execution of the asynchronous code hasn't completed!
            try step()            
            with | _ ->
                fin()
                reraise()
        match step with
        | Return _ as i ->
            fin()
            i
        | ReturnFrom t ->
            let awaitable = t.GetAwaiter()
            Await {Completion = awaitable; Continuation = (fun () ->
                    try awaitable.GetResult() |> Result.success |> Return
                    with | _ ->
                        fin()
                        reraise())}
        | Await async ->
            Await {async with Continuation = (fun () -> tryFinally async.Continuation fin)}
        | Lazy deferred ->
            lazy(tryFinally (fun () -> deferred.Value) fin) |> EventingLazy |> Lazy

    /// Implements a using statement that disposes `disp` after `body` has completed.
    let inline using (disp : #IDisposable) (body : _ -> OperationStep<'result,'event>) =
        // A using statement is just a try/finally with the finally block disposing if non-null.
        tryFinally
            (fun () -> body disp)
            (fun () -> if not (isNull (box disp)) then disp.Dispose())

    /// Implements a loop that runs `body` for each element in `sequence`.
    let forLoop (sequence : 'a seq) (body : 'a -> OperationStep<unit,'event>) =
        // A for loop is just a using statement on the sequence's enumerator...
        using (sequence.GetEnumerator())
            // ... and its body is a while loop that advances the enumerator and runs the body on each element.
            (fun e -> whileLoop e.MoveNext (fun () -> body e.Current))

    /// Runs an OperationStep as a task -- with a short-circuit for immediately completed steps.
    let rec run (firstStep : unit -> OperationStep<'result,'event>) =
        try
            match firstStep() with
            | Return x -> Completed x
            | ReturnFrom t -> InProcess { Task = t; EventsSoFar = [] }
            | Await _ as step -> InProcess { Task = OperationStepStateMachine<'result,'event>(step).Run().Unwrap(); EventsSoFar = [] } // sadly can't do tail recursion
            | Lazy deferred -> lazy((fun () -> deferred.Value) |> run) |> Operation.defer
        // Any exceptions should become a Completed Failure Operation, rather than being thrown from this call.
        // This matches C# Task Async behavior where you won't see an exception until awaiting the task,
        // even if it failed before reaching the first "await".
        with | ex ->
            Operation.ofException<'result,'event> ex

    /// Runs an OperationStep as a task -- with a short-circuit for immediately completed steps.
    let rec runEx (firstStep : unit -> OperationStep<'result,exn>) =
        try
            match firstStep() with
            | Return x -> Completed x
            | ReturnFrom t -> InProcess { Task = t; EventsSoFar = [] }
            | Await _ as step -> InProcess { Task = OperationStepStateMachine<'result,exn>(step).Run().Unwrap(); EventsSoFar = [] } // sadly can't do tail recursion
            | Lazy deferred -> lazy((fun () -> deferred.Value) |> runEx) |> Operation.defer
        // Any exceptions should become a Completed Failure Operation, rather than being thrown from this call.
        // This matches C# Task Async behavior where you won't see an exception until awaiting the task,
        // even if it failed before reaching the first "await".
        with | ex ->
            Completed <| Failure [ex]

    /// Lazily Evaluate an OperationStep
    let runLazy (firstStep : unit -> OperationStep<'result,'event>) =
        lazy(run firstStep) |> Operation.defer

    /// Return Operations from other Opreations
    let rec returnOp (op: Operation<'result,'event>) =
        match op with
        | Completed result -> Return result            
        | InProcess inProcess ->
            let awt = inProcess.Task.ConfigureAwait(false).GetAwaiter()
            if awt.IsCompleted 
            then Return (Result.success <| awt.GetResult())  // Proceed to the next step based on the result we already have.                
            else Await {Completion = awt; Continuation = (fun () -> Return (Result.success <| awt.GetResult()))} // Await and continue later when a result is available.
        | Deferred deferred -> returnOp deferred.Value // Force Evaluation in ReturnFrom
        | Cancelled events -> Return <| Failure events

    type UnitTask =
        struct
            val public Task : Task
            new(task) = { Task = task }
            member this.GetAwaiter() = this.Task.GetAwaiter()
            member this.ConfigureAwait(continueOnCapturedContext) = this.Task.ConfigureAwait(continueOnCapturedContext)
        end

    type OperationBuilder() =
        member inline __.Delay(f : unit -> OperationStep<_,_>) = f
        member inline __.Run(f : unit -> OperationStep<'result,'event>) = run f
        member inline __.Zero() = zero ()
        member inline __.Return(x) = retEx x
        member inline __.Return(s: SuccessfulResult<_,_>) = ret s
        member inline __.ReturnFrom(f: OperationResult<_,_>) = Return f
        member inline __.ReturnFrom(task : _ Task) = ReturnFrom task
        member inline __.ReturnFrom(s: SuccessfulResult<_,_>) = Return <| Success s
        member inline __.ReturnFrom(o: Operation<_,_>) = returnOp o
        member inline __.Combine(step : OperationStep<unit,'event>, continuation) = combine step continuation
        member inline __.While(condition : unit -> bool, body : unit -> OperationStep<unit,'event>) = whileLoop condition body
        member inline __.For(sequence : _ seq, body : _ -> OperationStep<unit,'event>) = forLoop sequence body
        member inline __.TryWith(body : unit -> OperationStep<_,_>, catch : exn -> OperationStep<_,_>) = tryWith body catch
        member inline __.TryFinally(body : unit -> OperationStep<_,_>, fin : unit -> unit) = tryFinally body fin
        member inline __.Using(disp : #IDisposable, body : #IDisposable -> OperationStep<_,_>) = using disp body
        member inline __.Bind(task : 'a Task, continuation : 'a -> OperationStep<'b,'event>) : OperationStep<'b,'event> =
            bindTaskNoContext task continuation
        member inline __.Bind(op : Operation<'a,'event>, continuation : 'a -> OperationStep<'b,'event>) : OperationStep<'b,'event> =
            bind op continuation
        member inline __.Bind(result : OperationResult<'a,'event>, continuation : 'a -> OperationStep<'b,'event>) : OperationStep<'b,'event> =
            bindResult result continuation
        member inline __.Bind(async: 'a Async, continuation: 'a -> OperationStep<'b,'event>): OperationStep<'b,'event> =
            bindAsync async continuation

    type LazyOperationBuilder() =
        member inline __.Delay(f : unit -> OperationStep<_,_>) = f
        member inline __.Run(f : unit -> OperationStep<'result,'event>) = runLazy f
        member inline __.Zero() = zero ()
        member inline __.Return(x) = retEx x
        member inline __.Return(s: SuccessfulResult<_,_>) = ret s
        member inline __.ReturnFrom(f: OperationResult<_,_>) = Return f
        member inline __.ReturnFrom(task : _ Task) = ReturnFrom task
        member inline __.ReturnFrom(s: SuccessfulResult<_,_>) = Return <| Success s
        member inline __.ReturnFrom(o: Operation<_,_>) = returnOp o
        member inline __.Combine(step : OperationStep<unit,'event>, continuation) = combine step continuation
        member inline __.While(condition : unit -> bool, body : unit -> OperationStep<unit,'event>) = whileLoop condition body
        member inline __.For(sequence : _ seq, body : _ -> OperationStep<unit,'event>) = forLoop sequence body
        member inline __.TryWith(body : unit -> OperationStep<_,_>, catch : exn -> OperationStep<_,_>) = tryWith body catch
        member inline __.TryFinally(body : unit -> OperationStep<_,_>, fin : unit -> unit) = tryFinally body fin
        member inline __.Using(disp : #IDisposable, body : #IDisposable -> OperationStep<_,_>) = using disp body
        member inline __.Bind(task : 'a Task, continuation : 'a -> OperationStep<'b,'event>) : OperationStep<'b,'event> =
            bindTaskNoContext task continuation
        member inline __.Bind(op : Operation<'a,'event>, continuation : 'a -> OperationStep<'b,'event>) : OperationStep<'b,'event> =
            bind op continuation
        member inline __.Bind(result : OperationResult<'a,'event>, continuation : 'a -> OperationStep<'b,'event>) : OperationStep<'b,'event> =
            bindResult result continuation
        member inline __.Bind(async: 'a Async, continuation: 'a -> OperationStep<'b,'event>): OperationStep<'b,'event> =
            bindAsync async continuation

[<AutoOpen>]
module ContextInsensitive =
    /// Builds a `System.Threading.Tasks.Task<'a>` similarly to a C# async/await method, but with
    /// all awaited tasks automatically configured *not* to resume on the captured context.
    /// This is often preferable when writing library code that is not context-aware, but undesirable when writing
    /// e.g. code that must interact with user interface controls on the same thread as its caller.
    let operation = OperationBuilder.OperationBuilder()
    let lazy_operation = OperationBuilder.LazyOperationBuilder()

    [<Obsolete("It is no longer necessary to wrap untyped System.Thread.Tasks.Task objects with \"unitTask\".")>]
    let inline unitTask (t : Task) = t.ConfigureAwait(false)

    // These are fallbacks when the Bind and ReturnFrom on the builder object itself don't apply.
    // This is how we support binding arbitrary task-like types.
    type OperationBuilder.OperationBuilder with
        member inline this.ReturnFrom(taskLike) =
            OperationBuilder.Binder<_,_>.GenericAwaitCaptureContext(taskLike, OperationBuilder.ret)
        member inline this.Bind(taskLike, continuation : _ -> OperationBuilder.OperationStep<'result,'event>) : OperationBuilder.OperationStep<'result,'event> =
            OperationBuilder.Binder<'result,'event>.GenericAwaitCaptureContext(taskLike, continuation)
    
    [<AutoOpen>]
    module HigherPriorityBinds =
        // When it's possible for these to work, the compiler should prefer them since they shadow the ones above.
        type OperationBuilder.OperationBuilder with
            member inline this.ReturnFrom(configurableTaskLike) =
                OperationBuilder.Binder<_,_>.GenericAwaitNoContext(configurableTaskLike, OperationBuilder.ret)
            member inline this.Bind(configurableTaskLike, continuation : _ -> OperationBuilder.OperationStep<'result,'event>) : OperationBuilder.OperationStep<'result,'event> =
                OperationBuilder.Binder<'result,'event>.GenericAwaitNoContext(configurableTaskLike, continuation)