namespace CurryOn.Common

open System
open System.Threading
open System.Threading.Tasks

module private TaskMonad =
    let bind<'t,'u> (v: Task<'t>) (map: 't -> Task<'u>) =
        v.ContinueWith(fun (task: Task<'t>) -> map task.Result).Unwrap()

    let identity<'t> (v: 't) = Task.FromResult v

    let unit () = Task.CompletedTask

    let delay<'t> (f: unit -> Task<'t>) = (fun () -> f())

    let run<'t> (f: unit -> Task<'t>) = 
        f()

    let combine<'t> (v1: Task<'t>) (v2: Task<'t>) =
        v1.ContinueWith(fun (task1: Task<'t>) -> 
            if task1.IsSuccessful
            then v2
            else task1).Unwrap()

    let rec whileLoop<'t> (guard: unit -> bool) (body: unit -> Task<'t>) =
        if guard()
        then 
            let task = body ()
            task.ContinueWith(fun (t: Task<'t>) -> 
                if t.IsSuccessful
                then whileLoop guard body
                else t).Unwrap()            
        else Task<'t>.FromResult(Unchecked.defaultof<'t>)

    let rec forLoop<'t,'u> (items: 't seq) (map: 't -> Task<'u>) =
        match items |> Seq.tryHead with
        | Some item -> 
            let task = map item
            task.ContinueWith(fun (t: Task<'u>) -> 
                if t.IsSuccessful
                then forLoop (items |> Seq.tail) map
                else t).Unwrap()
        | None -> Task<'u>.FromResult(Unchecked.defaultof<'u>)
    
    let tryWith<'t> (body: unit -> Task<'t>) (handler: exn -> Task<'t>) =
        try body()
        with | ex -> handler ex

    let tryFinally<'t> (body: unit -> Task<'t>) (compensation: unit -> unit) =
        try body()
        finally compensation()

    let using<'t,'d when 'd :> System.IDisposable and 'd: null and 'd: equality> (disposable: 'd) (body: 'd -> Task<'t>) =
        tryFinally (fun () -> body disposable) (fun () -> if disposable <> null then disposable.Dispose())

type TaskBuilder () =
    member __.Bind (v,map) = TaskMonad.bind v map
    member __.Delay f = TaskMonad.delay f
    member __.Run f = TaskMonad.run f
    member __.Return v = TaskMonad.identity v
    member __.ReturnFrom v = v
    member __.Yield v = TaskMonad.identity v
    member __.YieldFrom v = v
    member __.Zero () = TaskMonad.unit()
    member __.Combine (v1,v2) = TaskMonad.combine v1 v2
    member __.TryWith (body,handler) = TaskMonad.tryWith body handler
    member __.TryFinally (body,compensation) = TaskMonad.tryFinally body compensation
    member __.While (guard,body) = TaskMonad.whileLoop guard body
    member __.For (items,body) = TaskMonad.forLoop items body
    member __.Using (disposable,body) = TaskMonad.using disposable body

[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Task =
    let task = TaskBuilder()
    let ofUnit (t: Task) = 
        task {
            let result = Task.Run<unit>(fun () -> t.Wait())
            return! result
        }

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