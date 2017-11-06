namespace CurryOn.Common

open System.Collections.Generic

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