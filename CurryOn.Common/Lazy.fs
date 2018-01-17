namespace CurryOn.Common

open System
   
module private LazyMonad =
    let bind<'t,'u> (v: Lazy<'t>) (map: 't -> Lazy<'u>) =
        map (v.Value)

    let identity<'t> (v: 't) = lazy(v)

    let unit () = lazy ()

    let delay<'t> (f: unit -> Lazy<'t>) = 
        (fun () -> lazy (f().Value))

    let run<'t> (f: unit -> Lazy<'t>) = 
        lazy(f().Value)

    let combine<'t> (v1: Lazy<'t>) (v2: Lazy<'t>) =
        lazy (
            if v1.IsValueCreated
            then v1.Value
            else v2.Value)

    let rec whileLoop<'t> (guard: unit -> bool) (body: unit -> Lazy<'t>) =
        if guard()
        then whileLoop guard body
        else unit ()                

    let forLoop<'t,'u> (items: 't seq) (map: 't -> Lazy<'u>) =
        seq {
            for item in items do
                yield map item
        }
    
    let tryWith<'t> (body: unit -> Lazy<'t>) (handler: exn -> Lazy<'t>) =
        try body()
        with | ex -> handler ex

    let tryFinally<'t> (body: unit -> Lazy<'t>) (compensation: unit -> unit) =
        try body()
        finally compensation()

    let using<'t,'d when 'd :> System.IDisposable and 'd: null> (disposable: 'd) (body: System.IDisposable -> Lazy<'t>) =
        tryFinally (fun () -> body disposable) (fun () -> ifNotNull disposable (fun d -> d.Dispose()))


type LazyBuilder () =
    member __.Bind (v,map) = LazyMonad.bind v map
    member __.Delay f = LazyMonad.delay f
    member __.Run f = LazyMonad.run f
    member __.Return v = LazyMonad.identity v
    member __.ReturnFrom v = v
    member __.Yield v = LazyMonad.identity v
    member __.YieldFrom v = v
    member __.Zero () = LazyMonad.unit()
    member __.Combine (v1,v2) = LazyMonad.combine v1 v2
    member __.TryWith (body,handler) = LazyMonad.tryWith body handler
    member __.TryFinally (body,compensation) = LazyMonad.tryFinally body compensation
    member __.While (guard,body) = LazyMonad.whileLoop guard body
    member __.For (items,body) = LazyMonad.forLoop items body
    member __.Using (disposable,body) = LazyMonad.using disposable body

[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Lazy =
    let defer = LazyBuilder()

        
    
    

