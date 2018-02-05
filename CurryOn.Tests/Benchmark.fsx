#r @"bin\Debug\CurryOn.FSharp.Control.dll"
#r @"bin\Debug\CurryOn.Common.dll"
#r @"C:\Projects\GitHub\CurryOn\packages\FsPickler.3.2.0\lib\net45\FsPickler.dll"
#r @"C:\Projects\GitHub\CurryOn\packages\FsPickler.Json.3.2.0\lib\net45\FsPickler.Json.dll"
#r @"..\packages\EventStore.Client.4.0.3\lib\net40\EventStore.ClientAPI.dll"

open CurryOn.Common
open EventStore.ClientAPI
open FSharp.Control
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open System
open System.IO
open System.Net

let test number action =
    let stopwatch = Stopwatch()
    stopwatch.Start()
    {0..number} |> Seq.iter action
    stopwatch.Stop()
    stopwatch.Elapsed

let testMap number action =
    let stopwatch = Stopwatch()
    stopwatch.Start()
    {0..number} |> action
    stopwatch.Stop()
    stopwatch.Elapsed

let test100 () = test 100 <| fun i -> printfn "Testing #%d" i

let test1000Tasks () = test 1000 <| fun i -> task { printfn "Testing #%d" i } |> Task.runSynchronously

let test1000Ops () = test 1000 <| fun i -> operation { printfn "Testing #%d" i } |> Operation.returnOrFail

let testMap100Tasks () = testMap 1 <| fun values -> 
    values |> Seq.map (fun i -> 
        let i = 1
        task { 
            do! Task.Delay(10) |> Task.ofUnit
            printfn "Testing #%d" i     
        })
        |> Task.Parallel |> Task.ignoreSynchronously

let testMap100Ops () = testMap 100 <| fun values -> 
    values |> Seq.map (fun i -> 
        operation { 
            do! Async.Sleep 10
            printfn "Testing #%d" i 
        }) 
        |> Operation.Parallel |> Async.Ignore |> Async.RunSynchronously


test100()
test1000Tasks()
test1000Ops()

testMap100Tasks()
testMap100Ops()


let serializer = MBrace.FsPickler.Json.JsonSerializer(indent = true)
let expectedVersion = ref ExpectedVersion.NoStream
let credentials = new SystemData.UserCredentials("admin", "changeit")

let toJsonBytes any = 
    use stream = new MemoryStream()
    use writer = new StreamWriter(stream)
    serializer.Serialize(writer, any)
    stream.ToArray()

type TestEvent =
    {
        Name: string
        Value: int64
        Date: System.DateTime
        IsEvent: bool
        Id: System.Guid
    }

type MetaEvent = | Good | Bad of exn

let connection = EventStoreConnection.Create(IPEndPoint(IPAddress.Parse("127.0.0.1"), 1113))
do connection.ConnectAsync() |> Task.ofUnit |> Task.runSynchronously

let writeEvent stream (event: TestEvent) =
    operation {        
        let eventBytes = event |> toJsonBytes
        let eventData = EventData(Guid.NewGuid(), event.GetType().FullName, true, eventBytes, [||])
        let! result = connection.AppendToStreamAsync(stream, ExpectedVersion.Any |> int64, [|eventData|], credentials)
        return Result.successWithEvents result [Good]
    }

#time
[|0..100|] |> Array.Parallel.map (fun s ->
    let stream = sprintf "Test-%dJ" s
    {0L..1000L} |> Seq.map (fun x -> writeEvent stream {Name = sprintf "Event %d" x; Value = x; Date = DateTime.Now; IsEvent = true; Id = Guid.NewGuid()}) |> Operation.Parallel |> Async.RunSynchronously)
#time

