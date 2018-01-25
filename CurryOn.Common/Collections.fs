namespace CurryOn.Common

open System.Collections.Concurrent
open System.Collections.Generic

type IMessageQueue<'message> =
    abstract member Enqueue: 'message -> unit
    abstract member Dequeue: unit -> 'message option
    abstract member Length: int with get
    abstract member IsEmpty: bool with get
    abstract member HasMessages: bool with get

type IAsyncMessageQueue<'message> =
    inherit IMessageQueue<'message>
    abstract member DequeueAsync: unit -> Async<'message>
    abstract member Enqueued: IEvent<'message*int>

type MessageQueue<'message>() =
    let queue = new ConcurrentQueue<'message>()

    let enqueue message = queue.Enqueue message
    
    let dequeue () = 
        match queue.TryDequeue() with
        | (true, message) -> Some message
        | _ -> None

    let length () = queue.Count
    let isEmpty () = queue.IsEmpty
    let hasMessages () = queue.IsEmpty |> not


    member __.Enqueue message = enqueue message
    member __.Dequeue () = dequeue ()
    member __.Length with get () = length ()
    member __.IsEmpty with get () = isEmpty ()
    member __.HasMessages with get () = hasMessages ()

    interface IMessageQueue<'message> with
        member this.Enqueue message = this.Enqueue message
        member this.Dequeue () = this.Dequeue()
        member this.Length = this.Length
        member this.IsEmpty = this.IsEmpty
        member this.HasMessages = this.HasMessages

type AsyncMessageQueue<'message>() =
    let syncRoot = obj()
    let queue = ConcurrentQueue<'message>()
    let enqueued, triggerEnqueued = 
        let enqueuedEvent = Event<'message*int>()
        (enqueuedEvent.Publish, fun args -> enqueuedEvent.Trigger args)    

    let enqueue message =
        lock syncRoot <| fun () ->
            queue.Enqueue message
            triggerEnqueued (message,queue.Count)
    
    let dequeue () =
        lock syncRoot <| fun () -> 
            let rec dequeueAsync () =
                async {
                    if queue.IsEmpty
                    then do! enqueued |> Async.AwaitEvent |> Async.Ignore
                    match queue.TryDequeue() with
                    | (true, message) -> return message
                    | _ -> return! dequeueAsync ()                
                }
            dequeueAsync ()

    let tryDequeue () = 
        lock syncRoot <| fun () ->
            match queue.TryDequeue() with
            | (true, message) -> Some message
            | _ -> None

    let length () = lock syncRoot <| fun () -> queue.Count
    let isEmpty () = lock syncRoot <| fun () -> queue.IsEmpty
    let hasMessages () = lock syncRoot <| fun () -> queue.IsEmpty |> not

    member __.Enqueued = enqueued
    member __.Enqueue message = enqueue message
    member __.Dequeue () = dequeue ()
    member __.TryDequeue () = tryDequeue ()
    member __.Length with get () = length ()
    member __.IsEmpty with get () = isEmpty ()
    member __.HasMessages with get () = hasMessages ()


    interface IAsyncMessageQueue<'message> with
        member this.Enqueued = this.Enqueued
        member this.Enqueue message = this.Enqueue message
        member this.Dequeue () = this.TryDequeue ()
        member this.DequeueAsync () = this.Dequeue()
        member this.Length = this.Length
        member this.IsEmpty = this.IsEmpty
        member this.HasMessages = this.HasMessages


type private ListOperations<'t> =
| Add of 't*AsyncReplyChannel<unit>
| Clear of AsyncReplyChannel<unit>
| Contains of 't*AsyncReplyChannel<bool>
| Count of AsyncReplyChannel<int>
| CopyTo of 't[]*int*AsyncReplyChannel<unit>
| GetEnumerator of AsyncReplyChannel<IEnumerator<'t>>
| Remove of 't*AsyncReplyChannel<bool>


type IndexedList<'t, 'index when 'index: equality> (getIndex: 't -> 'index) =
    let items = List<'t>()    
    let index = Dictionary<'index, int>()

    let add item =
        items.Add item
        index.Add(getIndex item, items.Count - 1)

    let clear () =
        items.Clear()
        index.Clear()

    let contains item =
        index.ContainsKey(getIndex item)

    let count () = items.Count

    let copyTo array startIndex =
        items.CopyTo(array, startIndex)

    let getEnumerator () = items.GetEnumerator()

    let remove item =
        let rec removeAll () =
            if items.Remove item
            then removeAll ()
        removeAll ()
        index.Remove(getIndex item)

    let listActor = MailboxProcessor.Start <| fun inbox ->
        let rec receiveLoop () = 
            async {
                let! operation = inbox.Receive()
                match operation with
                | Add (item, replyChannel) -> add item |> replyChannel.Reply
                | Clear replyChannel -> clear () |> replyChannel.Reply
                | Contains (item, replyChannel) -> contains item |> replyChannel.Reply
                | Count replyChannel -> count () |> replyChannel.Reply
                | CopyTo (array, startIndex, replyChannel) -> copyTo array startIndex |> replyChannel.Reply
                | GetEnumerator replyChannel -> getEnumerator () |> replyChannel.Reply
                | Remove (item, replyChannel) -> remove item |> replyChannel.Reply
                return! receiveLoop ()
            }
        receiveLoop ()

    let post = listActor.PostAndAsyncReply

    member __.Count = count()
    member __.Add item = add item
    member __.Clear () = clear()
    member __.Contains item = contains item
    member __.CopyTo (array, startIndex) = copyTo array startIndex
    member __.GetEnumerator () = getEnumerator()
    member __.Remove item = remove item

    member __.CountAsync () = post <| fun replyChannel -> Count replyChannel
    member __.AddAsync item = post <| fun replyChannel -> Add (item, replyChannel)
    member __.ClearAsync () = post <| fun replyChannel -> Clear replyChannel
    member __.ContainsAsync item = post <| fun replyChannel -> Contains (item, replyChannel)
    member __.CopyToAsync (array, startIndex) = post <| fun replyChannel -> CopyTo (array, startIndex, replyChannel)
    member __.GetEnumeratorAsync () = post <| fun replyChannel -> GetEnumerator replyChannel
    member __.RemoveAsync item = post <| fun replyChannel -> Remove (item, replyChannel)
    
    interface ICollection<'t> with
        member ic.Count = ic.CountAsync () |> Async.RunSynchronously
        member ic.IsReadOnly = false
        member ic.Add item = ic.AddAsync item |> Async.RunSynchronously
        member ic.Clear () = ic.ClearAsync () |> Async.RunSynchronously
        member ic.Contains item = ic.ContainsAsync item |> Async.RunSynchronously
        member ic.CopyTo (array, startIndex) = ic.CopyToAsync (array, startIndex) |> Async.RunSynchronously
        member ic.GetEnumerator () = ic.GetEnumeratorAsync () |> Async.RunSynchronously
        member ic.GetEnumerator(): System.Collections.IEnumerator = ic.GetEnumeratorAsync () |> Async.RunSynchronously |> unbox<System.Collections.IEnumerator>
        member ic.Remove item = ic.RemoveAsync item |> Async.RunSynchronously