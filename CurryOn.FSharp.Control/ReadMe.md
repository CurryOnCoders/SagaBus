# CurryOn.FSharp.Control
The CurryOn.FSharp.Control library extends the FSharp.Control namespace with a framework for enabling the use of [Railway-Oriented Programming](https://fsharpforfunandprofit.com/rop/) patterns with the [Task Parallel Library (TPL)](https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/task-parallel-library-tpl), [Async Workflows](https://docs.microsoft.com/en-us/dotnet/fsharp/language-reference/asynchronous-workflows), and [Lazy Computations](https://docs.microsoft.com/en-us/dotnet/fsharp/language-reference/lazy-computations).  

This is accomplished by providing a set of types for working with **Operations** and their results.  An **Operation** is any function or expression that is intended to participate in the Railway-Oriented patterns, and is created by use of the `operation` Computation Expression.

```fsharp
open System.IO

let readFile (fileName: string) =
    operation {
        use fileStream = new StreamReader(fileName)
        return! fileStream.ReadToEndAsync()
    }    
```

The example above creates a function `val readFile : fileName:string -> Operation<string,exn>` that takes a file name and returns **Operation<string,exn>** representing the result of reading all text from the file.  The Operation type is a discriminated union with four cases:

```fsharp
type Operation<'result,'event> =
| Completed of Result: OperationResult<'result,'event>
| InProcess of IncompleteOperation: InProcessOperation<'result,'event>
| Deferred of Lazy: EventingLazy<Operation<'result,'event>>
| Cancelled of EventsSoFar: 'event list
```

The cases of the Operation discriminated union represent the possible states of the Operation after invocation.  Since the framework supports working with Tasks and Async Workflows, the Operation may not complete immediately, and may be cancelled, so the InProcess and Cancelled cases represent these states.  Since the framework supports working with Lazy computations, the Deferred case represents Operations in the state of waiting for a Lazy to be evaluated.

Operations that are not completed can be waited on synchronously using `Operation.wait`.  They can also be waited on with an F# Async using `Operation.waitAsync` or as a Task using `Operation.waitTask`.  These functions return the same type as the Completed case of the Operation discriminated union, `OperationResult<'result,'event>`.

```fsharp
type OperationResult<'result,'event> =
| Success of Result: SuccessfulResult<'result,'event>
| Failure of ErrorList: 'event list
```

The **OperationResult** type represents the result of a Completed Operation.  In the *readFile* example above, the result type would be `OperationResult<string,exn>`, since the resulting value is a `string`, and since the operation may throw exceptions, such as `FileNotFoundException`.  If no exceptions are thrown and the Operation completed successfully, the OperationResult will be the `Success` case, and the result will be contained within a `SuccessfulResult<'result,'event>`.  If any exception is thrown during the operation, the OperationResult will be the Failure case, and any exceptions thrown will be present in the list.

The `SuccessfulResult<'result,'event>` type is used to contain the resulting value and any domain events associated with a successful Operation.  The `SuccessfulResult` type also has members `.Result` and `.Events` to provide direct access to the result value and the domain events without pattern-matching.

```fsharp
type SuccessfulResult<'result,'event> =
| Value of ResultValue: 'result
| WithEvents of ResultWithEvents: SuccessfulResultWithEvents<'result,'event>
```

When no domain events are associated with the SuccessfulResult, the Value case will be used, and the `'result` will be directly accessible.  When a successful Operation also returns domain events, the results will be contained in a `SuccessfulResultWithEvents<'result,'event>` record type.

```fsharp
type SuccessfulResultWithEvents<'result,'event> =
    {
        Value: 'result
        Events: 'event list
    }
```

This allows the framework to support a usage pattern where a successful Operation can also return domain events, or carry Warnings or Informational messages along with the resulting value.  To use the framework in this way, it is common practice to create a discriminated union representing the possible errors, warnings, or domain events.  Then, the events can be propogated from one operation to another, such as in the following examples:

```fsharp
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
        return! if file.DirectoryName <> Environment.SystemDirectory
                then Result.success ()
                else Result.successWithEvents () [FileIsInSystemRootWarning]
    }
```

When used in this way, the Operation framework allows for informational messages or domain events to be propogated from one operation to another, such that calling `readFile` (with a file that exists) would return a Success with two events, FileOpenedSuccessfully and FileReadSuccessfully.  It also allows any known errors and warnings to be handled and returned from one Operation to another, terminating when a Failure is encountered without running Operations farther down the chain.  Any unforseen exceptions that may still be raised will be captured with the `UnhandledException` case.  It is recommended to include a case such as this in any discrimintaed union used for the `'event` type of an Operation, as the framework contains special logic to seek out a union case with a single field of type `exn` when an uhandled exception is thrown from an Operation.  This allows the exception to be captured and returned without changing the type of the Operation from `Operation<'result,'event>` to `Operation<'result,exn>`.  If the Operation is already of type `Operation<'result,exn>`, the unhandled exception is returned in the list of exceptions in the Failure case of the OperationResult.

### Railway-Oriented Programming
In addition to the Operation computation builder, the framework also includes the standard Railway-Oriented Programming functions and operators for working with `OperationResults` and a similar set for working with `Operations`.

```fsharp
type IPValidationEvent =
| IpAddressValidated
| InputStringWasNullOrEmpty
| SegmentNotNumerical of string
| SegmentOutOfRange of int
| InvalidNumberOfSegments of int
| UnexpectedError of exn

let validateNotNull s =
    if String.IsNullOrWhiteSpace s
    then Failure [InputStringWasNullOrEmpty]
    else Result.success s

let validateNumerical (s: string) =
    let isNumerical = s |> Seq.fold (fun acc cur -> acc && Char.IsDigit(cur)) true
    if isNumerical
    then Int32.Parse(s) |> Result.success
    else Failure [SegmentNotNumerical s]

let validateRange i =
    if i < 0 || i > 255
    then Failure [SegmentOutOfRange i]
    else Result.success <| Convert.ToByte i

let validateNumberOfSegments (s: string) =
    let segments = s.Split('.')
    if segments.Length = 4
    then Result.success segments
    else Failure [InvalidNumberOfSegments segments.Length]

let validateSegments (segments: string []) =
    segments 
    |> Array.map validateNumerical 
    |> Array.map (fun n -> n >>= validateRange)
    |> Result.join

let parseIpAddress inputString =
    inputString
    |> validateNotNull
    >>= validateNumberOfSegments
    >>= validateSegments
    >>= (fun segments -> Result.successWithEvents (Net.IPAddress segments) [IpAddressValidated])
```

When working with `Operations`, the operators typically have an additional character to differentiate them from the operators for working with `OperationResults`:

`bind`:   `>>=` for OperationResults, `>>>=` for Operations

`apply`:  `<*>` for OperationResults, `<**>` for Operations

`lift`:   `<!>` for OperationResults, `<!!>` for Operations

#### Working with Operations and OperationResults
To faciliate working with Operations and OperationResults, the framework provides a library of functions to simplify the interpretation, evaluation, and combination of Operations and their results.  

`Result.ok` can be used to test whether an OperationResult is successful.

`Operation.ok` can be used to test whether an entire Operation is successful.  This will force deferred Operations to evaluate and will synchronously wait for InProcess Operations to finish.
***

`Result.failed` can be used to test whether an OperationResult is a failure.

`Operation.failed` can be used to test whether an entire Operation has failed.  This will force deferred Operations to evaluate and will synchronously wait for InProcess Operations to finish.

`Operation.cancelled` can be used to test whether an Operation has been cancelled.  

`Operation.deferred` can be used to test whether an Operation is deferred for lazy evaluation.
***

`Result.ofOption` can be used to convert an `Option<'result>` into an `OperationResult<'result,'event>`, with the `Some value` case translating to `Success value` and the `None` case to `Failure []`

`Result.ofOptionWithEvent` can be used to convert an `Option<'result>` into an `OperationResult<'result,'event>`, with the `Some value` case translating to `Success value` and the `None` case to `Failure [event]` (the provided event is used for the None/Failure case).

`Result.ofOptionWithEvents` can be used to convert an `Option<'result>` into an `OperationResult<'result,'event>`, with the `Some value` case translating to `Success value` and the `None` case to `Failure events` (the provided events are used for the None/Failure case).
***

`Result.ofChoice` can be used to convert a `Choice<'result,'event>` into an `OperationResult<'result,'event>`, with the `Choice1of2 value` case translating to `Success value` and the `Choice2of2 event` case to `Failure [event]`

`Result.ofChoiceWithEvents` can be used to convert a `Choice<'result,'event>` into an `OperationResult<'result,'event>`, with the `Choice1of2 value` case translating to `Success value` and the `Choice2of2 events` case to `Failure events`
***

`Result.ofTask` can be used to convert a `Task<'result>` into a `Task<OperationResult<'result,'event>>`
***

`Result.ofException` can be used to convert any `System.Exception` to a Failed `OperationResult<'result,'event>`

`Operation.ofException` can be used to convert any `System.Exception` to a Completed Operation with a Failed `OperationResult<'result,'event>`
***

`Operation.complete` can be used to force an InProcess or Deferred Operation to complete, and waits for the result synchronously, returning a Completed Operation.

`Operation.completeAsync` can be used to force an InProcess or Deferred Operation to complete, and returns an `Async<Operation<'result,'event>>` where the Operation returned by the Async is guaranteed to be Completed.

`Operation.completeTask` can be used to force an InProcess or Deferred Operation to complete, and returns a `Task<Operation<'result,'event>>` where the Task's Result is guaranteed to be a Completed Operation.
***

`Result.join` can be used to convert a sequence of `OperationResults` into a single `OperationResult` where the value is an array.

`Operation.join` can be used to convert a sequence of `Operations` into a single `Operation` where the value is an array.

#### Interoperability
While the framework aims to make Operations easy to work with and combine to create larger Operations and entire programs, there may ultimately be a point where the program needs to either return a value or throw an exception, such as when interoperating with another library or with a user interface.  In this case, it is recommended to use the `Operation.returnOrFail` function to force evaluation of the Operation and either return the value of the successful result, or throw an exception with the failure events.

```fsharp
let copyFile inputFile outputFile =
    operation {
        let! fileText = readFile inputFile
        let fileBytes = fileText |> System.Text.Encoding.UTF8.GetBytes
        return! writeFile outputFile fileBytes
    }

copyFile "input.txt" "output.txt" |> Operation.returnOrFail
```

If the preceeding example is executed in F# interactive, assuming the files "input.txt" and "output.txt" don't exist, the result would be an exception similar to the following:

```
System.Exception: FileNotFound "C:\Users\userName\AppData\Local\Temp\input.txt"
   at Microsoft.FSharp.Core.Operators.FailWith[T](String message)
   at <StartupCode$FSI_0005>.$FSI_0005.main@()
```

In this way, an exception with a meaningful message is returned to a user or to a caller from an external system without having to share a library of domain events or convert between domain events and exceptions in both directions.

#### Parallel Execution
Similar to Tasks and Async Workflows, Operations can be executed in parallel to offer enhanced performance when multiple operations need to be executed and the operations are not interdependent.  This is accomplished by use of the `Operation.Parallel` function, as in the following example:

```fsharp
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
```

This returns an `Async<OperationResult<string,exn> []>`.  Passing the result of `Operation.Parallel` into `Async.RunSynchronously` returns an array of results with the HTML strings of each successful request.

#### Asynchronous and Lazy Operations
By default, Operations start to execute immediatley.  If the Operation Computation contains only synchronous code and does not call any other Operations, it will likely execute fully and return a Completed Operation.  If the Operation Computation calls any Task or Async-returning methods, or if it calls other Operations, the execution of the Operation will be paused while waiting for the Task/Async or the other Operation to complete, and the Computation will generally return an InProcess Operation.  If there is a need to asynchronously start an Operation containing only synchronous code, such as a long-running Operation that should not block the current thread, the `start_operation` Computation can be used instead:

```fsharp
let asyncOp =
    start_operation {
        return 
            while true do
                if DateTime.Now.Second % 10 = 0
                then printfn "Still Running"
                Async.Sleep 1000 |> Async.RunSynchronously
    }
```
This will start the execution of the Operation in a new Task and immediately return an InProcess Operation, so that it does not block the calling thread.  Similarly, if there is a need to prevent an Operation from executing until the Result is evaluated, the `lazy_operation` Computation can be used.

```fsharp
let lazyOp =
    lazy_operation {
        return DateTime.Now
    }
```

This will immediately return a Deferred Operation, and will not be evaluated until `Operation.wait`, `Operation.complete`, or `Operation.returnOrFail` is called.

## Acknowledgements

This project is based on the [Railway-Oriented Programming](https://fsharpforfunandprofit.com/posts/recipe-part2/) series on [F# for Fun and Profit](https://fsharpforfunandprofit.com/) and previous work available on GitHub:

* Railway-Oriented APIs for working with Result types based on [Chessie](https://github.com/fsprojects/Chessie)
* Task-Parallel Execution via Computation Expression based on [TaskBuilder.fs](https://github.com/rspeele/TaskBuilder.fs)
