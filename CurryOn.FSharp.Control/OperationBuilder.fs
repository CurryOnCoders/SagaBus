namespace FSharp.Control

open System
open System.Runtime.CompilerServices
open System.Threading.Tasks

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module OperationBuilder =
    /// Convert a list of domain events into an Exception (for use when creating a failed task)
    let inline private failEx events = OperationFailedException(events)

    /// Extensions to Task<'a> to support creating failed tasks from exceptions and domain events
    type Task<'a> with
        static member FromException(ex: exn) =
            let tcs = TaskCompletionSource<'a>()
            tcs.SetException(ex)
            tcs.Task
        static member FromFailureEvents(events) =
            Task.FromException(events |> failEx)

    /// Represents the completion notification and continuation for an asynchronous operation
    [<Struct>]
    type AsyncOperation<'result,'event> =
        {
            Completion: ICriticalNotifyCompletion
            Continuation: unit -> OperationStep<'result,'event>
        }

    /// Represents the state of an operation, which can either be 
    /// asynchronously waiting for something with a continuation,
    /// deferred for lazy evaluaiton, or have already completed 
    /// and carry forward a return value and possible events.
    and [<Struct>] OperationStep<'result,'event> =
        | AsyncStep of Async: AsyncOperation<'result,'event>
        | CompletedStep of Operation: OperationResult<'result,'event>
        | InProcessStep of InProcess: InProcessOperation<'result,'event>
        | LazyStep of Deferred: EventingLazy<OperationStep<'result,'event>>

    /// Provides the mechanism for executing an Operation step-by-step using tasks and continuations.
    and OperationStepStateMachine<'result,'event>(firstStep: OperationStep<'result,'event>) as this =
        let methodBuilder = AsyncTaskMethodBuilder<InProcessOperation<'result,'event>>()
        let continuation = ref <| fun () -> firstStep
        let rec processStep step =
            match step with
            | CompletedStep result ->
                match result with
                | Success successfulResult -> methodBuilder.SetResult(Task.FromResult(successfulResult.Result, successfulResult.Events))
                | Failure events -> methodBuilder.SetException(failEx events)
                None
            | InProcessStep t ->
                methodBuilder.SetResult(t)
                None
            | AsyncStep async ->
                continuation := async.Continuation
                Some async.Completion
            | LazyStep deferred ->
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
    let inline zero () = CompletedStep <| Result.success()

    /// Used to return a value.
    let inline ret x = CompletedStep <| Success x

    /// Used to return a value.
    let inline retEx (x : 'a) = CompletedStep <| Result.success<'a,exn> x

    /// Merge events from a previous OperationStep into the next OperationStep
    let rec mergeEvents (step: OperationStep<'result,'event>) (events: 'event list) =
        match step with
        | CompletedStep result -> CompletedStep <| Result.mergeEvents result events
        | InProcessStep inProcess -> inProcess.ContinueWith(fun (task: Task<'result*'event list>) -> 
            if task.IsFaulted 
            then let taskEvents =
                    match Result.ofException task.Exception with
                    | Failure es -> es
                    | _ -> []
                 Task<'result*'event list>.FromFailureEvents (events @ taskEvents)
            elif task.IsCanceled
            then Task<'result*'event list>.FromFailureEvents events
            else let result,taskEvents = task.Result
                 Task<'result*'event list>.FromResult(result, (events @ taskEvents))).Unwrap() |> InProcessStep
        | AsyncStep async -> AsyncStep {async with Continuation = (fun () -> async.Continuation() |> mergeEvents <| events)}
        | LazyStep deferred -> lazy(mergeEvents deferred.Value events) |> EventingLazy |> LazyStep


    /// Binding function for operation results
    let inline bindResult (result: OperationResult<'a,'event>) (cont: ('a*'event list) -> OperationStep<'b,'event>) =
        match result with
        | Success successfulResult -> cont (successfulResult.Result,successfulResult.Events)
        | Failure errors -> CompletedStep <| Failure errors

    /// Binding function for operation results of different event types
    let inline bindResultAcross (result: OperationResult<'a,'aevent>) (cont: ('a*'aevent list) -> OperationStep<'b,'bevent>) =
        if typeof<'bevent>.IsAssignableFrom(typeof<'aevent>)
        then match result with
             | Success successfulResult ->
                match successfulResult with
                | Value value -> cont (successfulResult.Result,[])
                | WithEvents withEvents -> cont (successfulResult.Result, withEvents.Events)
             | Failure errors -> CompletedStep <| Failure (errors |> List.map unbox<'bevent>)
        else match result with
             | Success successfulResult -> 
                match successfulResult with
                | Value value -> cont (successfulResult.Result,[])
                | WithEvents withEvents -> cont (successfulResult.Result, withEvents.Events)
             | Failure errors -> CompletedStep <| (new OperationFailedException<'aevent>(errors) |> Result.ofException<'b, 'bevent>)

    /// Primary binding function for operations
    let rec bind (op: Operation<'a,'event>) (cont: ('a*'event list) -> OperationStep<'b,'event>) =
        match op with
        | Completed result -> 
            bindResult result cont
        | InProcess inProcess ->
            let awt = inProcess.ConfigureAwait(false).GetAwaiter()
            if awt.IsCompleted 
            then cont(awt.GetResult())  // Proceed to the next step based on the result we already have.                
            else AsyncStep {Completion = awt; Continuation = (fun () -> cont(awt.GetResult()))} // AsyncStep and continue later when a result is available.
        | Deferred deferred ->
            bind deferred.Value cont
        | Cancelled events -> 
            CompletedStep <| Failure events

    /// Binding function for operations of different event types
    let rec bindAcross (op: Operation<'a,'aevent>) (cont: ('a*'aevent list) -> OperationStep<'b,'bevent>) =
        match op with
        | Completed result -> 
            bindResultAcross result cont
        | InProcess inProcess ->
            let awt = inProcess.ConfigureAwait(false).GetAwaiter()
            if awt.IsCompleted 
            then cont(awt.GetResult())  // Proceed to the next step based on the result we already have.                
            else AsyncStep {Completion = awt; Continuation = (fun () -> cont(awt.GetResult()))} // AsyncStep and continue later when a result is available.
        | Deferred deferred ->
            bindAcross deferred.Value cont
        | Cancelled events -> 
            CompletedStep <| (new OperationFailedException<'aevent>(events) |> Result.ofException<'b, 'bevent>)
    
    /// Due to the way F# structural type constraints work, 
    /// these functions need to be inside a class to properly
    /// scope the 'result and 'event type parameters.
    type AsyncOperationBinder<'result,'event> =
        static member inline private GenericAwait< ^awaitable, ^awaiter, ^input
                                                    when ^awaitable : (member GetAwaiter: unit -> ^awaiter)
                                                    and  ^awaiter   :> ICriticalNotifyCompletion 
                                                    and  ^awaiter   : (member get_IsCompleted: unit -> bool)
                                                    and  ^awaiter   : (member GetResult: unit -> ^input) >
            (awaitable: ^awaitable, continuation: ^input -> OperationStep<'result,'event>) : OperationStep<'result,'event> =
                let awaiter = (^awaitable: (member GetAwaiter: unit -> ^awaiter) (awaitable)) // get an awaiter from the awaitable
                if (^awaiter: (member get_IsCompleted: unit -> bool) (awaiter)) 
                then continuation (^awaiter: (member GetResult: unit -> ^input) (awaiter)) // shortcut to continue immediately
                else AsyncStep {Completion = awaiter; Continuation = (fun () -> continuation (^awaiter: (member GetResult: unit -> ^input) (awaiter)))}

        static member inline GenericAwaitNoContext< ^taskLike, ^awaitable, ^awaiter, ^input
                                                     when ^taskLike : (member ConfigureAwait: bool -> ^awaitable)
                                                     and ^awaitable : (member GetAwaiter: unit -> ^awaiter)
                                                     and ^awaiter   :> ICriticalNotifyCompletion 
                                                     and ^awaiter   : (member get_IsCompleted: unit -> bool)
                                                     and ^awaiter   : (member GetResult: unit -> ^input) >
            (taskLike: ^taskLike, continuation: ^input -> OperationStep<'result,'event>) : OperationStep<'result,'event> =
                let awaitable = (^taskLike: (member ConfigureAwait: bool -> ^awaitable) (taskLike, false))
                AsyncOperationBinder<'result,'event>.GenericAwait(awaitable, continuation)

        static member inline GenericAwaitCaptureContext (awaitable, continuation) = 
            AsyncOperationBinder<'result,'event>.GenericAwait(awaitable, continuation)

    /// Special binding for Task<'a> that captures the current context for running the continuation. 
    /// Have to write this out by hand to avoid confusing the compiler
    /// trying to decide between satisfying the constraints with `Task` or `Task<'a>`.
    let inline bindTaskCaptureContext (task : 'a Task) (continuation : 'a -> OperationStep<'b,'event>) =
        let awaiter = task.GetAwaiter()
        if awaiter.IsCompleted 
        then let result = awaiter.GetResult()
             continuation result  // Proceed to the next step based on the result we already have.
        else AsyncStep {Completion = awaiter; Continuation = (fun () -> // AsyncStep and continue later when a result is available.
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
        else AsyncStep {Completion = awaiter; Continuation = (fun () ->  // AsyncStep and continue later when a result is available.
                let result = awaiter.GetResult()
                continuation result)}

    /// Special case for binding F# Async<'a> without having to always call Async.StartAsTask in the Operation computation
    let inline bindAsync (asyncVal: 'a Async) (continuation: 'a -> OperationStep<'b, 'event>) =
        bindTaskNoContext (asyncVal |> Async.StartAsTask) continuation

    /// Special case for binding an Async<OperationResult<'a,'b>> directly
    let bindAsyncResult (asyncVal: Async<OperationResult<'a,'event>>) (continuation: 'a -> OperationStep<'b, 'event>) =
        let task = 
            async {
                let! result = asyncVal
                match result with
                | Success successfulResult -> return successfulResult.Result
                | Failure errors -> return! errors |> Task.FromFailureEvents |> Async.AwaitTask
            } |> Async.StartAsTask
        bindTaskNoContext task continuation

    /// Special case for binding an Async<OperationResult<'a,'b> []> directly (supports `let! results = Operation.Parallel operations`)
    let bindParallel (asyncVal: Async<OperationResult<'a,'event> []>) (continuation: 'a list -> OperationStep<'b, 'event>) =
        let task = 
            async {
                let! results = asyncVal
                match results |> Result.collect with
                | Success successfulResult -> return successfulResult.Result
                | Failure errors -> return! errors |> Task.FromFailureEvents |> Async.AwaitTask
            } |> Async.StartAsTask
        bindTaskNoContext task continuation

    /// Special case for binding Lazy<'a> and implicitly creating a Deferred OperationStep
    let inline bindLazy (lazyVal: 'a Lazy) (continuation: 'a -> OperationStep<'b, 'event>) =
        if lazyVal.IsValueCreated
        then lazyVal.Value |> continuation // Short-circuit for already-evaluated lazies
        else lazy(lazyVal.Value |> continuation) |> EventingLazy |> LazyStep


    /// Chains together a step with its following step.
    /// Note that this requires that the first step has no result.
    /// This prevents constructs like `operation { return 1; return 2; }`.
    let rec combine (step : OperationStep<unit,'event>) (continuation : unit -> OperationStep<'result,'event>) =
        match step with
        | CompletedStep _ -> continuation()
        | InProcessStep t ->
            AsyncStep {Completion = t.GetAwaiter(); Continuation = continuation}
        | AsyncStep async ->
            AsyncStep {Completion = async.Completion; Continuation = (fun () -> combine (async.Continuation()) continuation)}
        | LazyStep deferred ->
            lazy(combine deferred.Value continuation) |> EventingLazy |> LazyStep

    /// Builds a step that executes the body while the condition predicate is true.
    let rec whileLoop (cond : unit -> bool) (body : unit -> OperationStep<unit,'event>) =
        if cond()  
        then let rec repeat () = // Create a self-referencing closure to test whether to repeat the loop on future iterations.
                if cond() then
                    let body = body()
                    match body with
                    | CompletedStep _ -> repeat()
                    | InProcessStep t -> AsyncStep {Completion = t.GetAwaiter(); Continuation = repeat}
                    | AsyncStep async -> AsyncStep {async with Continuation = (fun () -> combine (async.Continuation()) repeat)}
                    | LazyStep deferred -> lazy(whileLoop cond (fun () -> deferred.Value)) |> EventingLazy |> LazyStep
                else zero ()
             // Run the body the first time and chain it to the repeat logic.
             combine (body()) repeat
        else zero ()

    /// Wraps a step in a try/with. This catches exceptions both in the evaluation of the function
    /// to retrieve the step, and in the continuation of the step (if any).
    let rec tryWith(step : unit -> OperationStep<'result,'event>) (catch : exn -> OperationStep<'result,'event>) =
        try match step() with
            | CompletedStep _ as i -> i
            | InProcessStep t ->
                let awaitable = t.GetAwaiter()
                AsyncStep {Completion = awaitable; Continuation = (fun () ->
                    try let result,events = awaitable.GetResult() 
                        if events |> List.isEmpty
                        then Result.success result 
                        else Result.successWithEvents result events 
                        |> CompletedStep
                    with | exn -> catch exn)}
            | AsyncStep async -> AsyncStep {async with Continuation = (fun () -> tryWith async.Continuation catch)}
            | LazyStep deferred -> lazy(tryWith (fun () -> deferred.Value) catch) |> EventingLazy |> LazyStep
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
        | CompletedStep _ as i ->
            fin()
            i
        | InProcessStep t ->
            let awaitable = t.GetAwaiter()
            AsyncStep {Completion = awaitable; Continuation = (fun () ->
                    try 
                        let result,events = awaitable.GetResult() 
                        if events |> List.isEmpty
                        then result |> Result.success 
                        else Result.successWithEvents result events
                        |> CompletedStep
                    with | _ ->
                        fin()
                        reraise())}
        | AsyncStep async ->
            AsyncStep {async with Continuation = (fun () -> tryFinally async.Continuation fin)}
        | LazyStep deferred ->
            lazy(tryFinally (fun () -> deferred.Value) fin) |> EventingLazy |> LazyStep

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
            | CompletedStep x -> Completed x
            | InProcessStep t -> InProcess t
            | AsyncStep _ as step -> InProcess <| OperationStepStateMachine<'result,'event>(step).Run().Unwrap() // not quite tail recursion -- will have O(n) memory usage
            | LazyStep deferred -> lazy((fun () -> deferred.Value) |> run) |> Operation.defer
        // Any exceptions should become a Completed Failure Operation, rather than being thrown from this call.
        // This matches C# Task Async behavior where you won't see an exception until awaiting the task,
        // even if it failed before reaching the first "await".
        with | :? OperationFailedException<'event> as ex -> Completed <| Failure ex.Events
             | ex -> Operation.ofException<'result,'event> ex

    /// Runs an OperationStep as a task -- with a short-circuit for immediately completed steps.
    let rec runEx (firstStep : unit -> OperationStep<'result,exn>) =
        try
            match firstStep() with
            | CompletedStep x -> Completed x
            | InProcessStep t -> InProcess t
            | AsyncStep _ as step -> InProcess <| OperationStepStateMachine<'result,exn>(step).Run().Unwrap() // not tail recursive -- will have O(n) memory usage
            | LazyStep deferred -> lazy((fun () -> deferred.Value) |> runEx) |> Operation.defer
        // Any exceptions should become a Completed Failure Operation, rather than being thrown from this call.
        // This matches C# Task Async behavior where you won't see an exception until awaiting the task,
        // even if it failed before reaching the first "await".
        with | ex -> Completed <| Failure [ex]

    /// Lazily Evaluate an OperationStep
    let inline runLazyStep (firstStep : unit -> OperationStep<'result,'event>) =
        lazy(run firstStep) |> Operation.defer

    /// Execute an OperationStep in a Task to ensure it runs asynchronously and creates an InProcess peration
    let rec runAsTask (firstStep : unit -> OperationStep<'result,'event>) =
        InProcess <| Task.Run(fun () -> run firstStep).ContinueWith(fun (task: Task<Operation<'result,'event>>) ->
            if task.IsFaulted
            then Task<'result*'event list>.FromException(task.Exception)
            elif task.IsCanceled
            then Task.FromException <| OperationCanceledException()
            else let rec toTask operation =            
                     match operation with
                     | Completed result -> 
                        match result with
                        | Success successfulResult -> Task.FromResult(successfulResult.Result,successfulResult.Events)
                        | Failure errors -> Task.FromFailureEvents errors
                     | InProcess t -> t
                     | Cancelled events-> Task.FromFailureEvents events
                     | Deferred deferred -> toTask deferred.Value
                 toTask task.Result
            ).Unwrap()

    /// CompletedStep Operations from other Opreations
    let rec returnOp (op: Operation<'result,'event>) =
        match op with
        | Completed result -> CompletedStep result            
        | InProcess inProcess ->
            let awaiter = inProcess.ConfigureAwait(false).GetAwaiter()
            let inline getResult () =
                let result,events = awaiter.GetResult()
                if events |> List.isEmpty
                then Result.success result
                else Result.successWithEvents result events
                |> CompletedStep
            if awaiter.IsCompleted // Proceed to the next step based on the result we already have. 
            then getResult()
            else AsyncStep {Completion = awaiter; Continuation = (fun () -> getResult())} // AsyncStep and continue later when a result is available.
        | Deferred deferred -> returnOp deferred.Value // Force Evaluation in ReturnFrom
        | Cancelled events -> CompletedStep <| Failure events

    /// Allow Operation computations to reutrn! an Async<OperationResult<'result,'event>> directly
    let returnAsync<'result,'event> (asyncVal: Async<OperationResult<'result,'event>>) =
        let rawAsync = 
            async {
                let! result = asyncVal
                match result with
                | Success successfulResult -> return successfulResult.Result,successfulResult.Events
                | Failure errors -> return! errors |> Task.FromFailureEvents |> Async.AwaitTask
            }
        (Async.StartAsTask >> InProcessStep) rawAsync

    /// Allow Operation computations to reutrn! an Async<OperationResult<'result,'event> []> directly (supports `reutrn! Operation.Parallel operations`)
    let returnParallel<'result,'event> (asyncVal: Async<OperationResult<'result,'event> []>) =
        let rawAsync = 
            async {
                let! results = asyncVal
                match results |> Result.collect with
                | Success successfulResult -> return successfulResult.Result,successfulResult.Events
                | Failure errors -> return! errors |> Task.FromFailureEvents |> Async.AwaitTask
            }
        (Async.StartAsTask >> InProcessStep) rawAsync

    /// Allow Operation computations to return! a Task<'result> directly
    let returnTask<'result,'event> (task: Task<'result>) =
        bindTaskNoContext task (fun result -> CompletedStep <| Result.success<'result,'event> result)

    /// Allow Operation computations to return! a Lazy<'result> directly
    let returnLazy<'result,'event> (lazyVal: Lazy<'result>) =
        bindLazy lazyVal (fun result -> CompletedStep <| Result.success<'result,'event> result)

    type UnitTask =
        struct
            val public Task : Task
            new(task) = { Task = task }
            member this.GetAwaiter() = this.Task.GetAwaiter()
            member this.ConfigureAwait(continueOnCapturedContext) = this.Task.ConfigureAwait(continueOnCapturedContext)
        end

    type OperationBuilder () =
        abstract member Run: (unit -> OperationStep<'result,'event>) -> Operation<'result,'event>
        default __.Run(f : unit -> OperationStep<'result,'event>) = run f
        member inline __.Delay(f : unit -> OperationStep<_,_>) = f        
        member inline __.Zero() = zero ()
        member inline __.Return(x) = retEx x
        member inline __.Return(s: SuccessfulResult<_,_>) = ret s
        member inline __.ReturnFrom(f: OperationResult<_,_>) = CompletedStep f
        member inline __.ReturnFrom(task: _ Task) = InProcessStep task
        member inline __.ReturnFrom(async: _ Async) = (Async.StartAsTask >> InProcessStep) async
        member inline __.ReturnFrom(async: Async<OperationResult<_,_>>) = returnAsync async
        member inline __.ReturnFrom(async: Async<OperationResult<_,_> []>) = returnParallel async
        member inline __.ReturnFrom(task: Task<'a>) = returnTask<'a,exn> task
        member inline __.ReturnFrom(s: SuccessfulResult<_,_>) = CompletedStep <| Success s
        member inline __.ReturnFrom(o: Operation<_,_>) = returnOp o
        member inline __.ReturnFrom(l: _ Lazy) = returnLazy l
        member inline __.Combine(step : OperationStep<unit,'event>, continuation) = combine step continuation
        member inline __.While(condition : unit -> bool, body : unit -> OperationStep<unit,'event>) = whileLoop condition body
        member inline __.For(sequence : _ seq, body : _ -> OperationStep<unit,'event>) = forLoop sequence body
        member inline __.TryWith(body : unit -> OperationStep<_,_>, catch : exn -> OperationStep<_,_>) = tryWith body catch
        member inline __.TryFinally(body : unit -> OperationStep<_,_>, fin : unit -> unit) = tryFinally body fin
        member inline __.Using(disp : #IDisposable, body : #IDisposable -> OperationStep<_,_>) = using disp body
        member inline __.Bind(task : 'a Task, continuation : 'a -> OperationStep<'b,'event>) : OperationStep<'b,'event> =
            bindTaskNoContext task continuation        
        //member inline __.Bind(op : Operation<'a,'event>, continuation : 'a -> OperationStep<'b,'event>) : OperationStep<'b,'event> =
        //    bind op (fun (result,events) -> continuation result |> mergeEvents <| events)    
        member inline __.Bind(op : Operation<'a,'aevent>, continuation : 'a -> OperationStep<'b,'bevent>) : OperationStep<'b,'bevent> =
            if typeof<'aevent> = typeof<'bevent>
            then bind (op |> Operation.cast<'a,'aevent,'bevent>) (fun (result,events) -> continuation result |> mergeEvents <| (events |> Seq.cast<'bevent> |> Seq.toList))
            else bindAcross op (fun (result,_) -> continuation result)
        member inline __.Bind(result : OperationResult<'a,'event>, continuation : 'a -> OperationStep<'b,'event>) : OperationStep<'b,'event> =
            bindResult result (fun (a,events) -> continuation a |> mergeEvents <| events)
        member inline __.Bind(async: 'a Async, continuation: 'a -> OperationStep<'b,'event>): OperationStep<'b,'event> =
            bindAsync async continuation
        member inline __.Bind(async: Async<OperationResult<'a,'event>>, continuation : 'a -> OperationStep<'b,'event>) : OperationStep<'b,'event> =
            bindAsyncResult async continuation
        member inline __.Bind(async: Async<OperationResult<'a,'event> []>, continuation : 'a list -> OperationStep<'b,'event>) : OperationStep<'b,'event> =
            bindParallel async continuation
        member inline __.Bind(laz: 'a Lazy, continuation: 'a -> OperationStep<'b,'event>): OperationStep<'b,'event> =
            bindLazy laz continuation

    type AsyncOperationBuilder () =
        inherit OperationBuilder()
        override __.Run(f : unit -> OperationStep<'result,'event>) = runAsTask f

    type LazyOperationBuilder () =
        inherit OperationBuilder()
        override __.Run(f : unit -> OperationStep<'result,'event>) = runLazyStep f

[<AutoOpen>]
module OperationBindings =
    // Bindings for the Operation computation builders
    let operation = OperationBuilder.OperationBuilder()
    let start_operation = OperationBuilder.AsyncOperationBuilder()
    let lazy_operation = OperationBuilder.LazyOperationBuilder()

    // Fallbacks for when the Bind overloads on the OperationBuilder object itself don't match.
    // This supports binding arbitrary task-like types in the computation expression.    
    type OperationBuilder.OperationBuilder with
        member inline this.ReturnFrom(configurableTaskLike) =
            OperationBuilder.AsyncOperationBinder<_,_>.GenericAwaitNoContext(configurableTaskLike, OperationBuilder.ret)
        member inline this.Bind(configurableTaskLike, continuation : _ -> OperationBuilder.OperationStep<'result,'event>) : OperationBuilder.OperationStep<'result,'event> =
            OperationBuilder.AsyncOperationBinder<'result,'event>.GenericAwaitNoContext(configurableTaskLike, continuation)