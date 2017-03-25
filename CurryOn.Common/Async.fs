namespace CurryOn.Common

type AsyncResult<'t> = Async<Result<'t>>

module AsyncResult =
    let fromResult<'t> (r: Result<'t>) = async { return r }
    
    let fromAsync<'t> (a: Async<'t>) = 
        async {
            try 
                let! result = a
                return result |> Success
             with | ex ->
                return Failure ex
        }

    let toResult<'t> (asyncResult: AsyncResult<'t>) =
        asyncResult |> Async.RunSynchronously

    let toAsync<'t> (asyncResult: AsyncResult<'t>) =
        async {
            let! result = asyncResult
            return match result with
                   | Success value -> value
                   | Failure ex -> raise ex
        }

module private TryAsyncMonad =
    let bind<'t,'u> (v: AsyncResult<'t>) (map: 't -> AsyncResult<'u>) =
        async {
            let! result = v
            return! match result with
                    | Success value -> map value
                    | Failure ex -> Result<'u>.Failure ex |> AsyncResult.fromResult
        }

    let identity<'t> (v: 't) = Success v |> AsyncResult.fromResult

    let unit () = Success () |> AsyncResult.fromResult

    let delay<'t> (f: unit -> AsyncResult<'t>) = (fun () -> f())

    let run<'t> (f: unit -> AsyncResult<'t>) = 
        try f()
        with | ex -> Failure ex |> AsyncResult.fromResult

    let combine<'t> (v1: AsyncResult<'t>) (v2: AsyncResult<'t>) =
        async {
            let! result1 = v1
            return! match result1 with
                    | Success _ -> v2
                    | Failure ex -> Failure ex |> AsyncResult.fromResult
        }

    let rec whileLoop<'t> (guard: unit -> bool) (body: unit -> AsyncResult<'t>) =
        if guard()
        then whileLoop guard body
        else unit ()                

    let forLoop<'t,'u> (items: 't seq) (map: 't -> AsyncResult<'u>) =
        seq {
            for item in items do
                yield try map item
                      with | ex -> Failure ex |> AsyncResult.fromResult
        }
    
    let tryWith<'t> (body: unit -> AsyncResult<'t>) (handler: exn -> AsyncResult<'t>) =
        try body()
        with | ex -> handler ex

    let tryFinally<'t> (body: unit -> AsyncResult<'t>) (compensation: unit -> unit) =
        try body()
        finally compensation()

    let using<'t,'d when 'd :> System.IDisposable and 'd: null> (disposable: 'd) (body: System.IDisposable -> AsyncResult<'t>) =
        tryFinally (fun () -> body disposable) (fun () -> (fun x -> disposable.Dispose()) |> ifNotNull disposable)


type TryAsyncBuilder() =
    member __.Bind (v,map) = TryAsyncMonad.bind v map
    member __.Delay f = TryAsyncMonad.delay f
    member __.Run f = TryAsyncMonad.run f
    member __.Return v = TryAsyncMonad.identity v
    member __.ReturnFrom v = v
    member __.Yield v = TryAsyncMonad.identity v
    member __.YieldFrom v = v
    member __.Zero () = TryAsyncMonad.unit()
    member __.TryWith (body,handler) = TryAsyncMonad.tryWith body handler
    member __.TryFinally (body,compensation) = TryAsyncMonad.tryFinally body compensation
    member __.While (guard,body) = TryAsyncMonad.whileLoop guard body
    member __.For (items,body) = TryAsyncMonad.forLoop items body
    member __.Using (disposable,body) = TryAsyncMonad.using disposable body

[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TryAsync =
    let tryAsync = TryAsyncBuilder()

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module LazyAsync =
    // TODO: Implement a LazyAsync computation and module
    ()