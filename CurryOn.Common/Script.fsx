#load "Utils.fs"
#load "Result.fs"
#load "Lazy.fs"
#load "Task.fs"
#load "AsyncResult.fs"
#load "TaskResult.fs"

open CurryOn.Common
open CurryOn.Common.Security.Encryption
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
        do! Task.Delay(5000) |> TaskResult.ofTask
        let! intValue = intResult |> TaskResult.fromResult
        return Thread.CurrentThread.ManagedThreadId
    }

Thread.CurrentThread.ManagedThreadId
intTaskResult |> TaskResult.toResult
