    namespace CurryOn.Common
    
    open System.Collections.Generic
    
    type ImmutableQueue<'message> private (front: 'message list, rear: 'message list) = 
        let enqueue message =        
            match front, message::rear with
            | [], newRear -> ImmutableQueue(newRear |> List.rev, [])
            | _, newRear -> ImmutableQueue(front, newRear)
    
        let enqueueAll messages = 
            let orderedMessages = messages |> List.rev
            match front, orderedMessages@rear with
            | [], newRear -> ImmutableQueue(newRear |> List.rev, [])
            | _, newRear -> ImmutableQueue(front, newRear)
    
        let dequeue () = 
            match front with
            | message::tail -> 
                message, (match tail with
                          | [] -> ImmutableQueue(rear |> List.rev, [])
                          | _ -> ImmutableQueue(tail, rear))
            | _ -> failwith "Cannot dequeue from empty queue!"        
    
        let tryDequeue () =  
            match front with
            | message::tail -> 
                (message, (match tail with
                           | [] -> ImmutableQueue(rear |> List.rev, [])
                           | _ -> ImmutableQueue(tail, rear)))
                |> Some
            | _ -> None

        let reverse () = 
            match front with
            | [] -> ImmutableQueue(rear |> List.rev, [])
            | _ -> ImmutableQueue(front, rear)
    
        let getEnumerator () = 
            (seq {
                yield! front
                yield! rear |> List.rev
            }).GetEnumerator()  
    
        static member Empty = ImmutableQueue<'message>([], []) 
        static member From messages = ImmutableQueue<'message>(messages, [])
    
        member __.IsEmpty = front.IsEmpty && rear.IsEmpty
        member __.Length = front.Length + rear.Length
        member __.HasMessages = front.IsEmpty |> not
        member __.Enqueue message = enqueue message        
        member __.EnqueueAll messages = enqueueAll messages
        member __.Dequeue () = dequeue ()
        member __.TryDequeue () = tryDequeue()
        member __.Reverse () = reverse()
        member __.GetEnumerator () = getEnumerator()
    
        interface IEnumerable<'message> with
            member this.GetEnumerator () = this.GetEnumerator()  
    
        interface System.Collections.IEnumerable with
            member this.GetEnumerator () = this.GetEnumerator() :> System.Collections.IEnumerator
    
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module Queue =
        let empty<'message> = ImmutableQueue<'message>.Empty
    
        let inline enqueue message (queue: ImmutableQueue<'message>) = queue.Enqueue message 
        
        let inline enqueueAll messages (queue: ImmutableQueue<'message>) = queue.EnqueueAll messages
    
        let inline isEmpty (queue: ImmutableQueue<'message>) = queue.IsEmpty
    
        let inline length (queue: ImmutableQueue<'message>) = queue.Length
    
        let inline hasMessages (queue: ImmutableQueue<'message>) = queue.HasMessages
    
        let inline ofList messages = messages |> ImmutableQueue.From
            
        let inline ofSeq messages = messages |> Seq.toList |> ofList
    
        let inline dequeue (queue: ImmutableQueue<'message>) = queue.Dequeue()
    
        let inline tryDequeue (queue: ImmutableQueue<'message>) = queue.TryDequeue()
    
        let inline rev (queue: ImmutableQueue<'message>) = queue.Reverse()