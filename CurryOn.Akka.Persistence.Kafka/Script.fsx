#r @"..\packages\FSharp.Control.AsyncSeq.2.0.18\lib\net45\FSharp.Control.AsyncSeq.dll"
#r @"..\packages\Kafunk.0.1.13\lib\net45\Kafunk.dll"
#r @"..\CurryOn.FSharp.Control\bin\Release\CurryOn.FSharp.Control.dll"

open FSharp.Control
open Kafunk


let kafka = Kafka.connHost "qadhinternal01"

let metadata = Kafka.metadata kafka <| MetadataRequest([||]) |> Async.RunSynchronously

metadata.topicMetadata |> Array.map (fun m -> m.topicName)
metadata.topicMetadata |> Array.map (fun m -> m.partitionMetadata)

let offset = Kafka.offsetFetch kafka <| OffsetFetchRequest("logging-consumer", [|("log_messages", [|0|])|]) |> Async.RunSynchronously

let getProducer topic =
    operation {
        let config = ProducerConfig.create (topic, Partitioner.roundRobin, RequiredAcks.Local)
        let! producer = 
            async {
                printfn "Evaluating"
                return! Producer.createAsync kafka config
            }
        return producer
    }

let producer = getProducer "log_traces"

let myOp =
    operation {
        let! prod = producer
        printfn "Got Producer %A" prod
        return prod
    }


let consumerConfig = ConsumerConfig.create ("test-consumer", "log_messages")
let consumer = Consumer.create kafka consumerConfig

let allMessages = Consumer.streamRange consumer <| Map.ofList [0, (735702L, 735712L)] |> Async.RunSynchronously

allMessages |> Array.head |> (fun msgs -> msgs.messageSet.messages.Length)


let newOffset = Kafka.offsetFetch kafka <| OffsetFetchRequest("test-consumer", [|("log_messages", [|0|])|]) |> Async.RunSynchronously
newOffset.topics


open System

let queuePriorities = "10,60,20,10"

let tryParseInt s =
    match Int32.TryParse(s) with
    | true, value -> Some value
    | _ -> None

let priorities = queuePriorities.Split(',')
                 |> Array.map tryParseInt
                 |> Array.filter (fun opt -> opt.IsSome)
                 |> Array.map (fun opt -> opt.Value)
                 |> Array.toList // if necessary

type PriorityValidationEvent =
| ValuesExceed100 of int
| ValuesLessThan100 of int

let getPriorities (priorityString: string) =
    operation {
        let priorities = priorityString.Split(',')
                         |> Array.map tryParseInt
                         |> Array.filter (fun opt -> opt.IsSome)
                         |> Array.map (fun opt -> opt.Value)
                         |> Array.toList

        let totalWeight = priorities |> List.sum

        return!
            if totalWeight > 100
            then Result.failure [ValuesExceed100 totalWeight]
            elif totalWeight < 100
            then Result.successWithEvents priorities [ValuesLessThan100 totalWeight]
            else Result.success priorities
    }

getPriorities queuePriorities
getPriorities "10,20,30,60"