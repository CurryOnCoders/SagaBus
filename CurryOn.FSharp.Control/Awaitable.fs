namespace FSharp.Control

open System
open System.Threading
open System.Threading.Tasks

/// A standard representation of an awaitable action, such as an F# Async Workflow or a .NET Task
[<Struct>]
type Awaitable<'a> =
| AsyncWorkflow of async: Async<'a>
| DotNetTask of task: Task<'a>

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Awaitable =
    let private await f g = function
    | AsyncWorkflow a -> f a
    | DotNetTask t -> g t

    /// Convert an F# Async Workflow to an Awaitable
    let ofAsync = AsyncWorkflow

    /// Convert a .NET Event into an Awaitable
    let ofEvent event = Async.AwaitEvent event |> AsyncWorkflow

    /// Convert a .NET Task<T> into an Awaitable
    let ofTask = DotNetTask

    /// Convert a .NET Task into an Awaitable
    let ofUnitTask (t: Task) =
        t.ContinueWith(fun (task: Task) -> 
            let tcs = TaskCompletionSource<unit>()
            if task.Status = TaskStatus.Canceled
            then tcs.SetCanceled()
            elif task.Status = TaskStatus.Faulted
            then tcs.SetException(task.Exception)
            else tcs.SetResult()
            tcs.Task).Unwrap() |> DotNetTask

    /// Start an Awaitable, if it is not already running
    let start = await Async.Start <| fun t -> if t.Status = TaskStatus.Created then t.Start()

    /// Create an Awaitable that will sleep for the specified amount of time
    let sleep = Async.Sleep >> AsyncWorkflow
    
    /// Convert an Awaitable into an F# Async Workflow
    let toAsync<'a> : Awaitable<'a> -> Async<'a> = await id Async.AwaitTask

    /// Convert an Awaitable into a .NET Task<T>
    let toTask<'a> : Awaitable<'a> -> Task<'a>  = await Async.StartAsTask id

    /// Construct an Awaitable from an existing value
    let value<'a> : 'a -> Awaitable<'a> = async.Return >> AsyncWorkflow
    
    /// Synchronously wait for the Awaitable to complete and return the result
    let wait<'a> : Awaitable<'a> -> 'a = await Async.RunSynchronously <| fun t -> t.RunSynchronously(); t.Result
    
    /// Run a set of Awaitables in parallel and create a single Awaitable that returns all of the resutls in an array
    let Parallel<'a> : Awaitable<'a> seq -> Awaitable<'a []> = 
        Seq.map toAsync >> Async.Parallel >> AsyncWorkflow
        
    /// Monadic bind, extract the value from inside an Awaitable and pass it to the given function
    let bind f = function
    | AsyncWorkflow a -> 
        async.Bind(a, f >> toAsync) |> AsyncWorkflow        
    | DotNetTask t -> t.ContinueWith(fun (c: Task<_>) -> 
        (c.Result |> f |> toTask)).Unwrap() |> DotNetTask

    /// Delay the evaluation of the given function, wrapping it in an Awaitable
    let delay f = bind f (value ())

    /// Combine an Awaitable<unit> with an Awaitable<'a>, 
    /// running them sequentially and returning an Awaitable<'a>
    let combine a b = bind (fun () -> b) a

    /// Evaluate an Awaitable<'a> until the guard condition returns false
    let rec doWhile guard a = 
        if guard ()
        then bind (fun () -> doWhile guard a) a
        else Task.FromResult() |> DotNetTask

    /// Try to evaluate the given Awaitable function, then unconditionally run the `finally`
    let tryFinally fin (f: unit -> Awaitable<_>) =
        async.TryFinally(f() |> toAsync, fin) |> AsyncWorkflow

    /// Try to evaluate the given Awaitable function, running the `catch` if an exception is thrown
    let tryCatch catch (f: unit -> Awaitable<_>) =
        async.TryWith(f() |> toAsync, catch >> toAsync) |> AsyncWorkflow

    /// Scope the given IDisposable resource to the Awaitable function,
    /// disposing the resource when the Awaitable has completed
    let using (a: 'a :> IDisposable) (f: 'a -> Awaitable<_>) =
        let dispose =
            let mutable flag = 0
            fun () ->
                if Interlocked.CompareExchange(&flag, 1, 0) = 0 && a |> box |> isNull |> not
                then (a :> IDisposable).Dispose()
        tryFinally dispose (fun () -> bind f (value a))

    /// Evaluate the given Awaitable function for each element in the sequence
    let forEach (items: _ seq) f = 
        using (items.GetEnumerator()) (fun e -> doWhile (fun () -> e.MoveNext()) (delay <| fun () -> f e.Current))

    /// Ignore the result of an Awaitable<'a> and return an Awaitable<unit>
    let ignore<'a> : Awaitable<'a> -> Awaitable<unit> = bind (ignore >> value)

type AwaitableBuilder () =
    member inline __.Bind (x, f) = Awaitable.bind f x
    member inline __.Bind (a, f) = a |> AsyncWorkflow |> Awaitable.bind f
    member inline __.Bind (t, f) = t |> DotNetTask |> Awaitable.bind f    
    member inline __.Delay f = Awaitable.delay f
    member inline __.Return x = Awaitable.value x
    member inline __.ReturnFrom (x: Awaitable<_>) = x
    member inline __.ReturnFrom a = a |> AsyncWorkflow
    member inline __.ReturnFrom t = t |> DotNetTask
    member inline __.Zero () = async.Return() |> AsyncWorkflow
    member inline __.Combine (a, b) = Awaitable.combine a b
    member inline __.Combine (a, b) = Awaitable.combine (AsyncWorkflow a) b
    member inline __.Combine (a, b) = Awaitable.combine (DotNetTask a) b
    member inline __.While (g, a) = Awaitable.doWhile g a
    member inline __.For (s, f) = Awaitable.forEach s f
    member inline __.TryWith (f, c) = Awaitable.tryCatch c f
    member inline __.TryFinally (f, fin) = Awaitable.tryFinally fin f
    member inline __.Using (d, f) = Awaitable.using d f
    member inline __.Using (d, f) = Awaitable.using d (f >> AsyncWorkflow)
    member inline __.Using (d, f) = Awaitable.using d (f >> DotNetTask)

[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module AwaitableBuilder =
    let await = AwaitableBuilder()

