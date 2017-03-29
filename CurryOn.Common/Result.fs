namespace CurryOn.Common

type Result<'a> =
| Success of 'a
| Failure of exn
with 
    member result.IsSuccessful = 
        match result with
        | Success _ -> true
        | _ -> false
    member result.HasFailed =
        result.IsSuccessful |> not

type Result = Result<unit>

module Result =
    let ofOption optionValue = 
        match optionValue with
        | Some value -> Success value
        | None -> Failure <| exn "Option.None"

    let toOption result =
        match result with
        | Success value -> Some value
        | Failure _ -> None

module Option =
    let ofResult result =
        match result with
        | Success value -> Some value
        | Failure _ -> None

    let toResult optionValue =
        match optionValue with
        | Some value -> Success value
        | None -> Failure <| exn "Option.None"
    
module private TryMonad =
    let bind<'t,'u> (v: Result<'t>) (map: 't -> Result<'u>) =
        match v with
        | Success value -> map value
        | Failure ex -> Result<'u>.Failure ex

    let identity<'t> (v: 't) = Success v

    let unit () = Success ()

    let delay<'t> (f: unit -> Result<'t>) = (fun () -> 
        try f()
        with | ex -> Failure ex)

    let run<'t> (f: unit -> Result<'t>) = 
        try f()
        with | ex -> Failure ex

    let combine<'t> (v1: Result<'t>) (v2: Result<'t>) =
        match v1 with
        | Success _ -> v2
        | failure -> failure

    let rec whileLoop<'t> (guard: unit -> bool) (body: unit -> Result<'t>) =
        if guard()
        then whileLoop guard body
        else unit ()                

    let forLoop<'t,'u> (items: 't seq) (map: 't -> Result<'u>) =
        seq {
            for item in items do
                yield try map item
                      with | ex -> Failure ex
        }
    
    let tryWith<'t> (body: unit -> Result<'t>) (handler: exn -> Result<'t>) =
        try body()
        with | ex -> handler ex

    let tryFinally<'t> (body: unit -> Result<'t>) (compensation: unit -> unit) =
        try body()
        finally compensation()

    let using<'t,'d when 'd :> System.IDisposable and 'd: null> (disposable: 'd) (body: System.IDisposable -> Result<'t>) =
        tryFinally (fun () -> body disposable) (fun () -> ifNotNull disposable (fun d -> d.Dispose()))


type ResultBuilder () =
    member __.Bind (v,map) = TryMonad.bind v map
    member __.Delay f = TryMonad.delay f
    member __.Run f = TryMonad.run f
    member __.Return v = TryMonad.identity v
    member __.ReturnFrom v = v
    member __.Yield v = TryMonad.identity v
    member __.YieldFrom v = v
    member __.Zero () = TryMonad.unit()
    member __.Combine (v1,v2) = TryMonad.combine v1 v2
    member __.TryWith (body,handler) = TryMonad.tryWith body handler
    member __.TryFinally (body,compensation) = TryMonad.tryFinally body compensation
    member __.While (guard,body) = TryMonad.whileLoop guard body
    member __.For (items,body) = TryMonad.forLoop items body
    member __.Using (disposable,body) = TryMonad.using disposable body

[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Attempt =
    let attempt = ResultBuilder()

        
    
    

