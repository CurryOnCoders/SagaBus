#load "ConcurrentBag.fs"
#load "ConcurrentMap.fs"
#load "ConcurrentQueue.fs"
#load "ConcurrentStack.fs"

open FSharp.Collections.Concurrent

let bag = 
    ConcurrentBag.empty
    |> ConcurrentBag.add "Test 1"
    |> ConcurrentBag.add "Test 2"

printfn "Bag.Count = %d, Bag.IsEmpty = %A" (bag |> ConcurrentBag.count) (bag |> ConcurrentBag.isEmpty)

match bag |> ConcurrentBag.tryTake with
| Some i -> printfn "Took Item %s" i
| None -> printfn "Couldn't take item"

match bag |> ConcurrentBag.tryPeek with
| Some i -> printfn "Peeked Item %s" i
| None -> printfn "Couldn't peek item"



let map = ConcurrentMap.empty<int, string>
   
map |> ConcurrentMap.addOrUpdate 1 "Test 1" |> ignore
map |> ConcurrentMap.addOrUpdate 2 "Test 2" |> ignore

printfn "Map.Count = %d, Map.IsEmpty = %A" (map |> ConcurrentMap.count) (map |> ConcurrentMap.isEmpty)

match map |> ConcurrentMap.tryRemove 1 with
| Some i -> printfn "Took Item 1 %s" i
| None -> printfn "Couldn't take 1"

match map |> ConcurrentMap.tryGetValue 2 with
| Some i -> printfn "Got Item 2 %s" i
| None -> printfn "Couldn't get item 2"


let queue = 
    ConcurrentQueue.empty
    |> ConcurrentQueue.enqueue "Test 1"
    |> ConcurrentQueue.enqueue "Test 2"

printfn "Queue.Count = %d, Queue.IsEmpty = %A" (queue |> ConcurrentQueue.count) (queue |> ConcurrentQueue.isEmpty)

match queue |> ConcurrentQueue.tryDequeue with
| Some i -> printfn "Dequeued Item %s" i
| None -> printfn "Couldn't Dequeue item"

match queue |> ConcurrentQueue.tryPeek with
| Some i -> printfn "Peeked Item %s" i
| None -> printfn "Couldn't Peek item"

queue |> ConcurrentQueue.count


let stack = 
    ConcurrentStack.empty
    |> ConcurrentStack.push "Test 1"
    |> ConcurrentStack.push "Test 2"

printfn "Queue.Count = %d, Queue.IsEmpty = %A" (stack |> ConcurrentStack.count) (stack |> ConcurrentStack.isEmpty)

match stack |> ConcurrentStack.tryPop with
| Some i -> printfn "Popped Item %s" i
| None -> printfn "Couldn't Pop item"

match stack |> ConcurrentStack.tryPeek with
| Some i -> printfn "Peeked Item %s" i
| None -> printfn "Couldn't Peek item"

stack |> ConcurrentStack.count