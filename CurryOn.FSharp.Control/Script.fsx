#load "Operation.fs"
#load "OperationBuilder.fs"

open FSharp.Control
open System
open System.IO
open System.Threading.Tasks

let rng = Random()

let myFirstOp =
    operation {
        let x = 3
        let y = 5
        return x + y
    }

let mySecondOp =
    operation {
        let! x = myFirstOp
        return x
    }

let myTask = Task.Run(fun () -> rng.Next())

let myThirdOp =
    operation {
        let! x = myTask
        return x
    }

let myFourthOp =
    operation {
        let! x = mySecondOp
        let! y = myThirdOp
        return x + y
    }

let myFifthOp =
    operation {
        use writer = new StreamWriter(@"C:\Temp\Numbers.txt", false)
        for _ in [1..1000] do
            do! writer.WriteLineAsync(sprintf "%d" <| rng.Next())// rng.Next(1, 100))
        return writer.Flush()
    }

let mySixthOp =
    operation {
        let! start = myFifthOp
        use reader = new StreamReader(@"C:\Temp\Numbers.txt")
        let! numbers = reader.ReadToEndAsync()
        return numbers.Split([|"\r\n"|], StringSplitOptions.RemoveEmptyEntries) |> Seq.map Int32.Parse |> Seq.sum
    }

mySixthOp |> Operation.wait

let myAsync =
    async {
        let next = rng.Next()
        do! Async.Sleep 1000
        return next * 2
    }

let mySeventhOp =
    operation {
        let! x = myAsync
        let y = rng.Next()
        return x + y
    }

mySeventhOp |> Operation.wait