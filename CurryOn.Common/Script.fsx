#r "System.Configuration"
#r @"..\packages\FSharp.Quotations.Evaluator.1.0.7\lib\net40\FSharp.Quotations.Evaluator.dll"

#load "Utils.fs"
#load "Result.fs"
#load "Extensions.fs"
#load "Lazy.fs"
#load "Task.fs"
#load "Async.fs"

open CurryOn.Common
open CurryOn.Common.Security.Encryption
open System
open System.Threading
open System.Threading.Tasks

Text.getWordList """Hello, my name is "Aaron", and I'm a software architect."""

let encryptionAlgorithm = AES (128, "TestKey")
let aesEncrypt = encrypt encryptionAlgorithm
let aesDecrypt = decrypt encryptionAlgorithm

let encryptedString = aesEncrypt "TestInput"
let decryptedString = aesDecrypt encryptedString

let rng = System.Random()
let threshold = 3

let intResult =
    attempt {
        let x = rng.Next(0,5)
        return
            if x < 3 
            then failwithf "This attempt has failed because the random number generated (%d) is less than the threshold value (%d)" x threshold
            else x
    }

let lazyInt =
    defer {
        return rng.Next()
    }

lazyInt.IsValueCreated
lazyInt.Value

let intTask =
    task {
        do! Task.Delay(5000) |> Task.ofUnit
        return Thread.CurrentThread.ManagedThreadId
    }

intTask.IsCompleted
intTask.Result

let nestedIntTask =
    task {
        return! Task.Run(fun () -> 
            Task.Delay(5000).Wait()
            Thread.CurrentThread.ManagedThreadId)
    }

nestedIntTask.IsCompleted
nestedIntTask.Result


let asyncIntResult =
    tryAsync {
        do! (Async.Sleep(5000) |> AsyncResult.fromAsync)
        let! intValue = intResult |> AsyncResult.fromResult
        return Thread.CurrentThread.ManagedThreadId
    }


asyncIntResult |> AsyncResult.toResult

let intTaskResult =
    tryTask {
        do! Task.Delay(5000) |> TaskResult.ofUnit
        let! intValue = intResult |> TaskResult.fromResult
        return Thread.CurrentThread.ManagedThreadId
    }

Thread.CurrentThread.ManagedThreadId
intTaskResult |> TaskResult.toResult


module MemoizingCalculator = 
    type Operation =
    | Add of decimal * decimal
    | Subtract of decimal * decimal
    | Multiply of decimal * decimal
    | Divide of decimal * decimal
    
    let calculate = memoize <| fun operation ->
        match operation with
        | Add (addend,adder) ->
            printfn "Calculating %A + %A" addend adder
            addend + adder
        | Subtract (minuend,subtrahend) ->
            printfn "Calculating %A - %A" minuend subtrahend
            minuend - subtrahend
        | Multiply (multiplicand,multiplier) ->
            printfn "Calculating %A * %A" multiplicand multiplier
            multiplicand * multiplier
        | Divide (dividend,divisor) ->
            printfn "Calculating %A / %A" dividend divisor
            dividend / divisor
        
open MemoizingCalculator
        
calculate <| Add (2M, 3M)
calculate <| Divide (235M, 15M)
calculate <| Multiply (3284.099M, 523.51M)
calculate <| Subtract (158M, 78M)
calculate <| Multiply (3284.099M, 523.51M)
calculate <| Add (2M, 3M)
calculate <| Subtract (158M, 78.01M)
calculate <| Multiply (3284.099M, 523.51M)
calculate <| Divide (235M, 15M)
calculate <| Add (2M, 4M)
calculate <| Add (3M, 2M)
calculate <| Add (3589M, 8154M)

// Timing Result, 1st Run (i7-4790K 4.0GHZ / 16GB RAM) : Real: 00:01:00.427, CPU: 00:01:00.093, GC gen0: 3266, gen1: 886, gen2: 2
#time
{0M..1000000M} |> Seq.pairwise |> Seq.map (fun (x,y) -> calculate <| Multiply (x,y)) |> Seq.toList
#time
// Timing Result, 2nd Run (i7-4790K 4.0GHZ / 16GB RAM) : Real: 00:00:01.902, CPU: 00:00:01.953, GC gen0: 89, gen1: 10, gen2: 0

let multiParameterFunction = memoize <| fun (x, y, z) -> 
    printfn "Executing Multi-Parameter Memoized Function"
    (x * y) / z

multiParameterFunction (6,13,4)


#load "ImmutableQueue.fs"

open CurryOn.Common
open System.Threading

let mutable queue = Queue.empty<string>
let index = ref 0

let nextMessage () = sprintf "Message %d" (Interlocked.Increment index)

for i in [1..100] do
    queue <- queue.Enqueue <| nextMessage ()

queue <- [1..1000] |> List.map (fun _ -> nextMessage()) |> queue.EnqueueAll

while queue.IsEmpty |> not do
    let (message, remaining) = queue.Dequeue()
    printfn "Dequeued %s" message
    queue <- remaining

let rec dequeueAll (queue: string ImmutableQueue) =
    match queue.TryDequeue () with
    | Some (message, remaining) ->
       printfn "Dequeued %s" message
       dequeueAll remaining
    | None -> ()

dequeueAll queue

let newQueue =
    queue 
    |> Queue.enqueue (nextMessage())
    |> Queue.enqueue (nextMessage())
    |> Queue.dequeue
    |> (fun (_,q) -> q |> Queue.enqueue (nextMessage()))
    |> Queue.enqueue (nextMessage())
    |> Queue.dequeue
    |> (fun (_,q) -> q |> Queue.dequeue)
    |> (fun (_,q) -> q |> Queue.enqueue (nextMessage()))
    |> Queue.enqueueAll ([1..10] |> List.map (fun _ -> nextMessage()))
    |> Queue.dequeue
    |> (fun (_,q) -> q |> Queue.enqueue (nextMessage()))

dequeueAll newQueue