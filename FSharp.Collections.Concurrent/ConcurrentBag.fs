namespace FSharp.Collections.Concurrent

open System.Collections.Concurrent 

module ConcurrentBag =
    let empty<'item> = ConcurrentBag<'item>()
    
    let inline add item (bag: ConcurrentBag<_>) =
        bag.Add(item)
        bag

    let inline count (bag: ConcurrentBag<_>) =
        bag.Count

    let inline isEmpty (bag: ConcurrentBag<_>) =
        bag.IsEmpty

    let inline tryTake (bag: ConcurrentBag<_>) =
        match bag.TryTake() with
        | (true, item) -> Some item
        | _ -> None

    let inline tryPeek (bag: ConcurrentBag<_>) =
        match bag.TryPeek() with
        | (true, item) -> Some item
        | _ -> None

    let inline toSeq (bag: ConcurrentBag<_>) =
        seq { for element in bag -> element }
    
    let toList<'item> = toSeq >> Seq.toList<'item>

    let toArray<'item> = toSeq >> Seq.toArray<'item>

    let inline ofSeq items =
        ConcurrentBag(items)
    
    let ofList<'item> : List<'item> -> ConcurrentBag<'item> = ofSeq

    let ofArray<'item> : 'item [] -> ConcurrentBag<'item> = ofSeq
    
