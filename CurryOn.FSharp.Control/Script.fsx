#r "bin/Debug/CurryOn.FSharp.Control.dll"

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

let myFifthOp() =
    operation {
        use writer = new StreamWriter(@"C:\Temp\Numbers.txt", false)
        for _ in [1..1000] do
            do! writer.WriteLineAsync(sprintf "%d" <| rng.Next(1, 100))
        return writer.Flush()
    }

let mySixthOp =
    operation {
        let! start = myFifthOp()
        use reader = new StreamReader(@"C:\Temp\Numbers.txt")
        let! numbers = reader.ReadToEndAsync()
        return numbers.Split([|"\r\n"|], StringSplitOptions.RemoveEmptyEntries) |> Seq.map Int32.Parse |> Seq.sum
    }

mySixthOp |> Operation.wait

mySixthOp |> Operation.returnOrFail

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

mySeventhOp |> Operation.returnOrFail

let asyncRandom max =
    async {
        do! Async.Sleep 1000
        return rng.Next(1,max)
    }

let myEighthOp () =
    operation {
        let! x = asyncRandom 200
        if x > 100
        then return! Failure [exn "Too Large"]
        else return x
    }

let myNinthOp () =
    operation {
        let! x = asyncRandom 50
        return! 
            if x < 25
            then failwith "Too Small"
            else Result.successWithEvents x []
    }

let myTenthOp =
    operation {
        let! x = myEighthOp ()
        let! y = myNinthOp ()
        return x + y
    }

myTenthOp |> Operation.complete

myTenthOp |> Operation.completeAsync

let myEleventhOp () =
    operation {
        failwith "Runtime Error"
    }

let myTwelvthOp =
    operation {
        try do! myEleventhOp ()
            return 3
        with | ex ->
            return ex.Message.Length
    }

let myLazyOp () =
    lazy_operation {
        let x = rng.Next(1,10)
        let! y = asyncRandom(10)
        let z = x + y
        return if z > 10
               then failwithf "Over Limit: %d" z
               else z
    }

let myLazyOpRun = myLazyOp()

match myLazyOpRun with
| Deferred deferred -> deferred.Evaluated |> Event.add (fun value -> printfn "Evaluated %O" value)
| _ -> ()

myLazyOpRun |> Operation.returnOrFail

let myComposedOp =
    operation {
        let! x = asyncRandom(10)
        let! y = myLazyOpRun
        let z = x + y
        return if z > 20
               then failwithf "Over Combined Limit: %d" z
               else z
    }

myComposedOp |> Operation.returnOrFail

let myLazyLazyOp =
    lazy_operation {
        return! myLazyOp()
    }

myLazyLazyOp |> Operation.wait



type MyDomainEvents =
| GenericError
| InfoMessage
| WarningMessage
| NoErrors

let myFirstDomainOp () =
    operation {
        let x = 3
        let y = 5
        return! Result.successWithEvents (x + y) [InfoMessage]
    }

let mySecondDomainOp () = 
    operation {
        let! x = myFirstDomainOp()
        return! Result.successWithEvents x [WarningMessage]
    }

let myThirdDomainOp =
    operation {
        let! x = myFirstDomainOp()
        let! y = mySecondDomainOp()
        return! Result.successWithEvents (x + y) [NoErrors]
    }


/// ReadMe.md samples
open System.IO

//let readFile (fileName: string) =
//    operation {
//        use fileStream = new StreamReader(fileName)
//        return! fileStream.ReadToEndAsync()
//    }  

type FileAccessEvents =
| FileOpenedSuccessfully
| FileReadSuccessfully
| FileNotFound of string
| FileIsInSystemRootWarning
| UnhandledException of exn // This is returned automatically if an unhandled exception is thrown by an Operation

let getFile (fileName: string) =
    operation {
        let file = FileInfo fileName
        return! if not file.Exists
                then Result.failure [FileNotFound file.FullName]
                else Result.success file
    }

let openFile fileName =
    operation {
        let! file = getFile fileName
        return! file.OpenText() |> Result.successWithEvents <| [FileOpenedSuccessfully]
    }

let readFile fileName = 
    operation {
        use! fileStream = openFile fileName
        let! fileText = fileStream.ReadToEndAsync()
        return! Result.successWithEvents fileText [FileReadSuccessfully]
    }

let writeFile fileName contents =
    operation {
        let! file = getFile fileName
        let stream = file.OpenWrite()
        do! stream.AsyncWrite contents
        return! if file.DirectoryName = Environment.SystemDirectory
                then Result.success ()
                else Result.successWithEvents () [FileIsInSystemRootWarning]
    }

readFile <| @"C:\Temp\Numbers.txt"

let copyFile inputFile outputFile =
    operation {
        let! fileText = readFile inputFile
        let fileBytes = fileText |> System.Text.Encoding.UTF8.GetBytes
        return! writeFile outputFile fileBytes
    }

copyFile "input.txt" "output.txt" |> Operation.returnOrFail


#r "System.Net.Http"
open System.Net.Http

let fetchUrl (url: string) = 
    operation {
        use client = new HttpClient()
        return! client.GetStringAsync url
    }

[fetchUrl "http://www.microsoft.com";
 fetchUrl "http://www.google.com";
 fetchUrl "http://www.github.com";]
|> Operation.Parallel
|> Async.RunSynchronously