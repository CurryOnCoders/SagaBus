namespace FSharp.Collections.Concurrent

open System.Collections.Concurrent 

module ConcurrentStack =
    let empty<'item> = ConcurrentStack<'item>()
    
    let inline push item (stack: ConcurrentStack<_>) =
        stack.Push(item)
        stack

    let inline pushRange items (stack: ConcurrentStack<_>) =
        stack.PushRange(items)
        stack

    let inline clear (stack: ConcurrentStack<_>) =
        stack.Clear()
        stack

    let inline count (stack: ConcurrentStack<_>) =
        stack.Count

    let inline isEmpty (stack: ConcurrentStack<_>) =
        stack.IsEmpty

    let inline tryPop (stack: ConcurrentStack<_>) =
        match stack.TryPop() with
        | (true, item) -> Some item
        | _ -> None

    let inline tryPopRange items (stack: ConcurrentStack<_>) =
        stack.TryPopRange(items)

    let inline tryPeek (stack: ConcurrentStack<_>) =
        match stack.TryPeek() with
        | (true, item) -> Some item
        | _ -> None

    let inline toSeq (stack: ConcurrentStack<_>) =
        seq { for element in stack -> element }
    
    let toList<'item> = toSeq >> Seq.toList<'item>

    let toArray<'item> = toSeq >> Seq.toArray<'item>

    let inline ofSeq items =
        ConcurrentStack(items)
    
    let ofList<'item> : List<'item> -> ConcurrentStack<'item> = ofSeq

    let ofArray<'item> : 'item [] -> ConcurrentStack<'item> = ofSeq