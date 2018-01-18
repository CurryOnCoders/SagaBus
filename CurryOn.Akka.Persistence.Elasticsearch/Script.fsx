//#r @"..\packages\Newtonsoft.Json.10.0.1\lib\net45\Newtonsoft.Json.dll"
//#r @"..\packages\Akka.1.2.3\lib\net45\Akka.dll"
//#r @"..\packages\Akka.Persistence.1.2.3.43-beta\lib\net45\Akka.Persistence.dll"
//#r @"..\packages\Akka.Persistence.FSharp.1.2.3.43-beta\lib\net45\Akka.Persistence.FSharp.dll"
//#r @"..\packages\Elasticsearch.Net.5.6.0\lib\net46\Elasticsearch.Net.dll"
//#r @"..\packages\FsPickler.3.2.0\lib\net45\FsPickler.dll"
//#r @"..\packages\FsPickler.Json.3.2.0\lib\net45\FsPickler.Json.dll"
//#r @"..\packages\NEST.5.6.0\lib\net46\Nest.dll"
//#r "bin/debug/CurryOn.FSharp.Control.dll"
//#r "bin/debug/CurryOn.Elastic.FSharp.dll"
//#r "bin/debug/CurryOn.Akka.Persistence.Elasticsearch.dll"

//open Akka.Persistence.Elasticsearch
//open CurryOn.Elastic
//open FSharp.Control

//typeof<PersistedEvent>.GetCustomAttributes(typeof<Nest.ElasticsearchTypeAttribute>, true)

//let intOp = operation { return 3 }
//let decOp = operation { return 3M }
//let mixedOp = 
//    operation {
//        let! x = intOp
//        let! y = decOp
//        return sprintf "%d = %f" x y
//    }

type INumerics<'T> =
  abstract Zer : 'T
  abstract Add : 'T * 'T -> 'T
  abstract Sub : 'T * 'T -> 'T
  abstract Mul : 'T * 'T -> 'T
  abstract Div : 'T * 'T -> 'T
  abstract Neq : 'T * 'T -> bool

let inline add (x : 'T) (y : 'T) : 'T   = (+)  x y
let inline sub (x : 'T) (y : 'T) : 'T   = (-)  x y
let inline mul (x : 'T) (y : 'T) : 'T   = (*)  x y
let inline div (x : 'T) (y : 'T) : 'T   = (/)  x y
let inline neq (x : 'T) (y : 'T) : bool = (<>) x y

type Agent<'T> = MailboxProcessor<'T>

type CalculatorMsg<'T> =
    | Add of 'T * 'T * AsyncReplyChannel<'T>
    | Sub of 'T * 'T * AsyncReplyChannel<'T> 
    | Mul of 'T * 'T * AsyncReplyChannel<'T>  
    | Div of 'T * 'T * AsyncReplyChannel<'T>

let ops = 
            { new INumerics<'T> with 
                member ops.Zer       = LanguagePrimitives.GenericZero<'T> 
                member ops.Add(x, y) = (x, y) ||> add  
                member ops.Sub(x, y) = sub x y
                member ops.Mul(x, y) = (x, y) ||> mul   
                member ops.Div(x, y) = (x, y) ||> div   
                member ops.Neq(x, y) = (x, y) ||> neq }

type CalculatorAgent< 'T when 'T : (static member get_Zero : unit -> 'T) 
                         and  'T : (static member Zero : 'T) 
                         and  'T : (static member op_Addition  : 'T * 'T -> 'T)
                         and  'T : (static member op_Subtraction  : 'T * 'T -> 'T)
                         and  'T : (static member (*)  : 'T * 'T -> 'T)
                         and  'T : (static member op_Division  : 'T * 'T -> 'T)
                         and  'T : (static member (<>) : 'T * 'T -> bool)
                         and  'T : equality >() =
    let agent =
        let ops = 
            { new INumerics<'T> with 
                member ops.Zer       = LanguagePrimitives.GenericZero<'T> 
                member ops.Add(x, y) = (x, y) ||> add  
                member ops.Sub(x, y) = sub x y
                member ops.Mul(x, y) = (x, y) ||> mul   
                member ops.Div(x, y) = (x, y) ||> div   
                member ops.Neq(x, y) = (x, y) ||> neq }

        Agent<CalculatorMsg<'T>>.Start(fun inbox ->
            let rec loop () =
                async {
                    let! msg = inbox.TryReceive()
                    if msg.IsSome then
                        match msg.Value with 
                        | Add (x, y, rep) ->
                            printfn "Adding %A and %A ..." x y
                            let res = ops.Add(x, y)
                            res |> rep.Reply  
                            return! loop()
                        | Sub (x, y, rep) -> 
                            printfn "Subtracting %A from %A ..." y x
                            let res = ops.Sub(x, y) 
                            res |> rep.Reply  
                            return! loop()
                        | Mul (x, y, rep) ->
                            printfn "Multiplying %A by %A ... " y x
                            let res = ops.Mul(x, y)
                            res |> rep.Reply  
                            return! loop()
                        | Div (x, y, rep) ->
                            printfn "Dividing %A by %A ..." x y
                            if ops.Neq(y, ops.Zer) then 
                                let res = ops.Div(x, y)
                                res |> rep.Reply  
                            else
                                printfn "#DIV/0" 
                            return! loop()
                    else 
                        return! loop()
                }
            loop()
        )

    // timeout = infinit => t = -1
    let t = 1000

    member inline this.Add(x, y) =
        agent.PostAndTryAsyncReply((fun rep -> Add (x, y, rep)), t)
        |> Async.RunSynchronously
    member inline this.Subtract(x, y) =
        agent.PostAndTryAsyncReply((fun rep -> Sub (x, y, rep)), t)
        |> Async.RunSynchronously
    member inline this.Multiply(x, y) =
        agent.PostAndTryAsyncReply((fun rep -> Mul (x, y, rep)), t)
        |> Async.RunSynchronously
    member inline this.Divide(x, y) =
        agent.PostAndTryAsyncReply((fun rep -> Div (x, y, rep)), t)
        |> Async.RunSynchronously


let calculatorAgentI = new CalculatorAgent<int>()

(2, 1) |> calculatorAgentI.Add 
(2, 1) |> calculatorAgentI.Subtract
(2, 1) |> calculatorAgentI.Multiply
(2, 1) |> calculatorAgentI.Divide
(2, 0) |> calculatorAgentI.Divide