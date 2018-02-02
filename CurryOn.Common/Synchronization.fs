namespace CurryOn.Common

open System
open System.Threading

type LockWait =
| InfiniteWait
| WaitTime of TimeSpan
| WaitWithCancellation of CancellationToken
| WaitTimeWithCancellation of TimeSpan*CancellationToken

type LockStatus =
| LockAcquired
| LockCancelled
| LockTimeout
| LockUnavailable

type ISynchronizationPrimitive =
    inherit IDisposable
    abstract member Acquire: LockWait -> Async<LockStatus>
    abstract member AcquireImmediate: unit -> LockStatus
    abstract member Release: unit -> unit

type AsyncLock () =
    let isLocked, getLock, getLockImmediate, release =
        let lockObject = obj()
        let locked = ref false
        (fun () -> !locked),
        (fun () -> locked := true),
        (fun () -> if !locked 
                   then false 
                   else locked := true
                        !locked),
        (fun () -> locked := false)
    
    member __.Acquire lockWait = 
        async {
            return LockAcquired
        }

    member __.AcquireImmediate () =
        LockUnavailable

    member __.Release () = ()

    member this.Dispose () =
        this.Release()

    interface ISynchronizationPrimitive with
        member this.Acquire lockWait = this.Acquire lockWait
        member this.AcquireImmediate () = this.AcquireImmediate()
        member this.Release () = this.Release()
        member this.Dispose () = this.Dispose()
