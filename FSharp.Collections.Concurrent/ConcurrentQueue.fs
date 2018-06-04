namespace FSharp.Collections.Concurrent

open System.Collections.Concurrent 

module ConcurrentQueue =
    let empty<'item> = ConcurrentQueue<'item>()

    let inline enqueue item (queue: ConcurrentQueue<_>) =
        queue.Enqueue(item)
        queue

    let inline count (queue: ConcurrentQueue<_>) =
        queue.Count

    let inline isEmpty (queue: ConcurrentQueue<_>) =
        queue.IsEmpty

    let inline tryDequeue (queue: ConcurrentQueue<_>) =
        match queue.TryDequeue() with
        | (true, item) -> Some item
        | _ -> None

    let inline tryPeek (queue: ConcurrentQueue<_>) =
        match queue.TryPeek() with
        | (true, item) -> Some item
        | _ -> None

    let inline toSeq (queue: ConcurrentQueue<_>) =
        seq { for element in queue -> element }
    
    let toList<'item> = toSeq >> Seq.toList<'item>

    let toArray<'item> = toSeq >> Seq.toArray<'item>

    let inline ofSeq items =
        ConcurrentQueue(items)
    
    let ofList<'item> : List<'item> -> ConcurrentQueue<'item> = ofSeq

    let ofArray<'item> : 'item [] -> ConcurrentQueue<'item> = ofSeq

    