namespace CurryOn.Common

open System
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Task =
    let private fromCompletionSource<'a> action =
        let completionSource = TaskCompletionSource<'a>()
        completionSource |> action
        completionSource.Task

    let FromException (ex: exn) =
        fromCompletionSource <| fun completionSource -> completionSource.SetException(ex)

    let FromCancellation<'a> () =
        fromCompletionSource<'a> <| fun completionSource -> completionSource.SetCanceled()

    exception TaskEvaluationException of exn * AggregateException

    let ofUnit (t: Task) = 
        let completionSource = TaskCompletionSource<unit>()
        t.ContinueWith(fun (task: Task) ->
            try
                if task.IsSuccessful
                then completionSource.SetResult()
                else completionSource.SetException(task.Exception.Flatten())
                completionSource.Task
            finally
                task.Dispose()).Unwrap()

    let fromUnit (t: Task<unit>) =
        t :> Task

    let runSynchronously<'a> (task : Task<'a>) =
        task.Result

    let toResult<'a> (t: Task<'a>) =
        try t.Result |> Success
        with | ex -> TaskEvaluationException (ex, t.Exception.Flatten()) |> Failure

    let Parallel<'a> (tasks: Task<'a> seq) =
        Task<'a>.WhenAll(tasks)

    let Ignore<'a> (t: Task<'a>) =
        let completionSource = TaskCompletionSource<unit>()
        t.ContinueWith(fun (task: Task<'a>) ->
            try
                if task.IsSuccessful
                then completionSource.SetResult ()
                else completionSource.SetException(task.Exception.Flatten())
                completionSource.Task
            finally
                task.Dispose()).Unwrap()

    let ignoreSynchronously<'a> = Ignore<'a> >> runSynchronously

    let success value =
        fromCompletionSource <| fun completionSource -> completionSource.SetResult value

    let failure message =
        FromException <| exn message

    let cancellation<'a> () = 
        FromCancellation<'a> ()

type TaskStep<'result> =
| Value of 'result
| AsyncValue of 'result Task
| Continuation of ICriticalNotifyCompletion * (unit -> 'result TaskStep)

type TaskAsyncStateMachine<'result>(firstStep) as this =
    let methodBuilder = AsyncTaskMethodBuilder<'result Task>()
    let mutable continuation = fun () -> firstStep
    let self = ref this
    let nextAwaitable() =
        try
            match continuation() with
            | Value r ->
                methodBuilder.SetResult(Task.FromResult(r))
                null
            | AsyncValue t ->
                methodBuilder.SetResult(t)
                null
            | Continuation (await, next) ->
                continuation <- next
                await
        with
        | ex ->
            methodBuilder.SetException(ex)
            null

    member __.Run() =
        methodBuilder.Start(self)
        methodBuilder.Task
    
    interface IAsyncStateMachine with
        member __.MoveNext() =
            let await = ref <| nextAwaitable()
            if await.Value |> isNotNull
            then methodBuilder.AwaitUnsafeOnCompleted(await, self)    
        member __.SetStateMachine(_) = () 

module TaskMonad =
    let unwrapException (aggregateException : AggregateException) =
        if aggregateException.InnerExceptions.Count = 1
        then aggregateException.InnerExceptions |> Seq.head
        else aggregateException :> exn

    let zero = Value ()

    let inline ret (x : 'a) = Value x

    type Binder<'out> =
        static member inline GenericAwait< ^abl, ^awt, ^inp
                                            when ^abl : (member GetAwaiter : unit -> ^awt)
                                            and ^awt :> ICriticalNotifyCompletion 
                                            and ^awt : (member get_IsCompleted : unit -> bool)
                                            and ^awt : (member GetResult : unit -> ^inp) >
            (abl : ^abl, continuation : ^inp -> 'out TaskStep) : 'out TaskStep =
                let awt = (^abl : (member GetAwaiter : unit -> ^awt)(abl))
                if (^awt : (member get_IsCompleted : unit -> bool)(awt)) 
                then continuation (^awt : (member GetResult : unit -> ^inp)(awt))
                else Continuation (awt, fun () -> continuation (^awt : (member GetResult : unit -> ^inp)(awt)))

        static member inline GenericAwaitConfigureFalse< ^tsk, ^abl, ^awt, ^inp
                                                        when ^tsk : (member ConfigureAwait : bool -> ^abl)
                                                        and ^abl : (member GetAwaiter : unit -> ^awt)
                                                        and ^awt :> ICriticalNotifyCompletion 
                                                        and ^awt : (member get_IsCompleted : unit -> bool)
                                                        and ^awt : (member GetResult : unit -> ^inp) >
            (tsk : ^tsk, continuation : ^inp -> 'out TaskStep) : 'out TaskStep =
                let abl = (^tsk : (member ConfigureAwait : bool -> ^abl)(tsk, false))
                Binder<'out>.GenericAwait(abl, continuation)

    let inline bindTask (task : 'a Task) (continuation : 'a -> TaskStep<'b>) =
        let awt = task.GetAwaiter()
        if awt.IsCompleted 
        then continuation(awt.GetResult())
        else Continuation (awt, (fun () -> continuation(awt.GetResult())))

    let inline bindTaskConfigureFalse (task : 'a Task) (continuation : 'a -> TaskStep<'b>) =
        let awt = task.ConfigureAwait(false).GetAwaiter()
        if awt.IsCompleted 
        then continuation(awt.GetResult())
        else Continuation (awt, (fun () -> continuation(awt.GetResult())))

    let rec combine (step : TaskStep<unit>) (continuation : unit -> TaskStep<'b>) =
        match step with
        | Value _ -> continuation()
        | AsyncValue t -> Continuation (t.GetAwaiter(), continuation)
        | Continuation (awaitable, next) -> Continuation (awaitable, fun () -> combine (next()) continuation)

    let whileLoop (cond : unit -> bool) (body : unit -> TaskStep<unit>) =
        if cond() then
            let rec repeat () =
                if cond() then
                    let body = body()
                    match body with
                    | Value _ -> repeat()
                    | AsyncValue t -> Continuation(t.GetAwaiter(), repeat)
                    | Continuation (awaitable, next) -> Continuation (awaitable, fun () -> combine (next()) repeat)
                else zero
            combine (body()) repeat
        else zero

    let rec tryWith(step : unit -> TaskStep<'a>) (catch : exn -> TaskStep<'a>) =
        try
            match step() with
            | Value _ as i -> i
            | AsyncValue t ->
                let awaitable = t.GetAwaiter()
                Continuation(awaitable, fun () ->
                    try
                        awaitable.GetResult() |> Value
                    with
                    | exn -> catch exn)
            | Continuation (awaitable, next) -> Continuation (awaitable, fun () -> tryWith next catch)
        with
        | ex -> catch ex

    let rec tryFinally (step : unit -> TaskStep<'a>) fin =
        let step =
            try step()
            with| _ ->
                fin()
                reraise()
        match step with
        | Value _ as i ->
            fin()
            i
        | AsyncValue t ->
            let awaitable = t.GetAwaiter()
            Continuation(awaitable, fun () ->
                try awaitable.GetResult() |> Value
                with | _ ->
                    fin()
                    reraise())
        | Continuation (awaitable, next) ->
            Continuation (awaitable, fun () -> tryFinally next fin)

    let inline using (disp : #IDisposable) (body : _ -> TaskStep<'a>) =
        tryFinally
            (fun () -> body disp)
            (fun () -> if not (isNull (box disp)) then disp.Dispose())

    let forLoop (sequence : 'a seq) (body : 'a -> TaskStep<unit>) =
        using (sequence.GetEnumerator())
            (fun e -> whileLoop e.MoveNext (fun () -> body e.Current))

    let run (firstStep : unit -> TaskStep<'a>) =
        try match firstStep() with
            | Value x -> Task.FromResult(x)
            | AsyncValue t -> t
            | Continuation _ as step -> TaskAsyncStateMachine<'a>(step).Run().Unwrap()
        with | ex ->
            let src = new TaskCompletionSource<_>()
            src.SetException(ex)
            src.Task

type UnitTask =
    struct
        val public Task : Task
        new(task) = { Task = task }
        member this.GetAwaiter() = this.Task.GetAwaiter()
        member this.ConfigureAwait(continueOnCapturedContext) = this.Task.ConfigureAwait(continueOnCapturedContext)
    end

type TaskBuilder() =
    member inline __.Delay(f : unit -> TaskStep<_>) = f
    member inline __.Run(f : unit -> TaskStep<'m>) = TaskMonad.run f
    member inline __.Zero() = TaskMonad.zero
    member inline __.Return(x) = TaskMonad.ret x
    member inline __.ReturnFrom(task : _ Task) = AsyncValue task
    member inline __.Combine(step : unit TaskStep, continuation) = TaskMonad.combine step continuation
    member inline __.While(condition : unit -> bool, body : unit -> unit TaskStep) = TaskMonad.whileLoop condition body
    member inline __.For(sequence : _ seq, body : _ -> unit TaskStep) = TaskMonad.forLoop sequence body
    member inline __.TryWith(body : unit -> _ TaskStep, catch : exn -> _ TaskStep) = TaskMonad.tryWith body catch
    member inline __.TryFinally(body : unit -> _ TaskStep, fin : unit -> unit) = TaskMonad.tryFinally body fin
    member inline __.Using(disp : #IDisposable, body : #IDisposable -> _ TaskStep) = TaskMonad.using disp body
    member inline __.Bind(task : 'a Task, continuation : 'a -> 'b TaskStep) : 'b TaskStep = TaskMonad.bindTask task continuation

[<AutoOpen>]
module TaskExpression =
    let task = TaskBuilder()

// Active Patterns for Handling Task Results
module Tasks =
    let (|Succeeded|_|) (t: Task<_>) = 
        try 
            do t.Wait()
            if t.IsSuccessful
            then Some t.Result
            else None
        with | ex -> None

    let (|Failed|_|) (t: Task<_>) =
        try 
            do t.Wait()
            if t.IsFaulted
            then t.Exception :> exn |> Some
            else None
        with 
            | :? AggregateException as aggregate ->
                if aggregate.InnerExceptions.Count < 2
                then aggregate.InnerException |> Some
                else aggregate :> exn |> Some
            | :? OperationCanceledException as cancel -> None
            | ex -> Some ex

    let (|Cancelled|_|) (t: Task<_>) =
        try 
            do t.Wait()
            if t.IsCanceled
            then Some ()
            else None
        with 
            | :? OperationCanceledException as cancel -> Some ()
            | ex -> None
    

type TaskResult<'a> = Task<Result<'a>>

module TaskResult =
    let fromResult<'t> (r: Result<'t>) = task { return r }
    let ofUnit (t: Task) = 
        task {
            try 
                return! t.ContinueWith(fun (tr: Task) ->
                    if t.IsSuccessful
                    then Success ()
                    else Failure t.Exception)
            with | ex ->  return Failure ex
        }

    let fromTask<'t> (a: Task<'t>) = 
        task {
            try 
                return! a.ContinueWith(fun (tr: Task<'t>) ->
                    if tr.IsSuccessful
                    then Success (tr.Result)
                    else Failure (tr.Exception))
             with | ex ->
                return Failure ex
        }

    let toResult<'t> (taskResult: TaskResult<'t>) = taskResult.Result

    let toTask<'t> (taskResult: TaskResult<'t>) =
        task {
            let! result = taskResult
            return match result with
                   | Success value -> value
                   | Failure ex -> raise ex
        }

module private TryTaskMonad =
    let bind<'t,'u> (v: TaskResult<'t>) (map: 't -> TaskResult<'u>) =
        task {
            try 
                return! v.ContinueWith(fun (tr: Task<Result<'t>>) ->
                    if tr.IsSuccessful
                    then match tr.Result with
                         | Success value -> map value
                         | Failure ex -> Result<'u>.Failure ex |> TaskResult.fromResult
                    else Result<'u>.Failure (tr.Exception) |> TaskResult.fromResult).Unwrap()
            with | ex -> return Failure ex
        }

    let identity<'t> (v: 't) = Success v |> TaskResult.fromResult

    let unit () = Success () |> TaskResult.fromResult

    let delay<'t> (f: unit -> TaskResult<'t>) = (fun () -> 
        try f()
        with | ex -> Failure ex |> TaskResult.fromResult)

    let run<'t> (f: unit -> TaskResult<'t>) = 
        try f().ContinueWith(fun (tr: Task<Result<'t>>) ->
            if tr.IsSuccessful
            then tr.Result
            else Failure tr.Exception)            
        with | ex -> Failure ex |> TaskResult.fromResult

    let combine<'t> (v1: TaskResult<'t>) (v2: TaskResult<'t>) =
        task {
            try return! v1.ContinueWith(fun (tr1: Task<Result<'t>>) ->
                if tr1.IsSuccessful
                then v2.ContinueWith(fun (tr2: Task<Result<'t>>) ->
                    if tr2.IsSuccessful
                    then tr2.Result
                    else Failure tr2.Exception)
                else Failure tr1.Exception |> TaskResult.FromResult).Unwrap()                                
            with | ex -> return Failure ex
        }

    let rec whileLoop<'t> (guard: unit -> bool) (body: unit -> TaskResult<'t>) =
        if guard()
        then whileLoop guard body
        else unit ()                

    let forLoop<'t,'u> (items: 't seq) (map: 't -> TaskResult<'u>) =
        seq {
            for item in items do
                yield try map item
                      with | ex -> Failure ex |> TaskResult.fromResult
        }
    
    let tryWith<'t> (body: unit -> TaskResult<'t>) (handler: exn -> TaskResult<'t>) =
        try
            try 
                body().ContinueWith(fun (tr: Task<Result<'t>>) ->
                    if tr.IsSuccessful 
                    then tr.Result
                    else Failure tr.Exception)
            with | ex -> handler ex
        with | ex -> Failure ex |> TaskResult.fromResult

    let tryFinally<'t> (body: unit -> TaskResult<'t>) (compensation: unit -> unit) =
        try
            try
               body().ContinueWith(fun (tr: Task<Result<'t>>) ->
                    if tr.IsSuccessful 
                    then tr.Result
                    else Failure tr.Exception)
            finally compensation()
        with | ex -> Failure ex |> TaskResult.fromResult

    let using<'t,'d when 'd :> System.IDisposable and 'd: null and 'd: equality> (disposable: 'd) (body: 'd -> TaskResult<'t>) =
        tryFinally (fun () -> body disposable) (fun () -> if disposable <> null then disposable.Dispose())

type TryTaskBuilder() =
    member __.Bind (v,map) = TryTaskMonad.bind v map
    member __.Delay f = TryTaskMonad.delay f
    member __.Run f = TryTaskMonad.run f
    member __.Return v = TryTaskMonad.identity v
    member __.ReturnFrom v = v
    member __.Yield v = TryTaskMonad.identity v
    member __.YieldFrom v = v
    member __.Zero () = TryTaskMonad.unit()
    member __.Combine (v1,v2) = TryTaskMonad.combine v1 v2
    member __.TryWith (body,handler) = TryTaskMonad.tryWith body handler
    member __.TryFinally (body,compensation) = TryTaskMonad.tryFinally body compensation
    member __.While (guard,body) = TryTaskMonad.whileLoop guard body
    member __.For (items,body) = TryTaskMonad.forLoop items body
    member __.Using (disposable,body) = TryTaskMonad.using disposable body

[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TryTask =
    let tryTask = TryTaskBuilder()