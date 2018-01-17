#r @"..\packages\Newtonsoft.Json.10.0.1\lib\net45\Newtonsoft.Json.dll"
#r @"..\packages\Akka.1.2.3\lib\net45\Akka.dll"
#r @"..\packages\Akka.Persistence.1.2.3.43-beta\lib\net45\Akka.Persistence.dll"
#r @"..\packages\Akka.Persistence.FSharp.1.2.3.43-beta\lib\net45\Akka.Persistence.FSharp.dll"
#r @"..\packages\Elasticsearch.Net.5.6.0\lib\net46\Elasticsearch.Net.dll"
#r @"..\packages\FsPickler.3.2.0\lib\net45\FsPickler.dll"
#r @"..\packages\FsPickler.Json.3.2.0\lib\net45\FsPickler.Json.dll"
#r @"..\packages\NEST.5.6.0\lib\net46\Nest.dll"
#r "bin/debug/CurryOn.FSharp.Control.dll"
#r "bin/debug/CurryOn.Elastic.FSharp.dll"
#r "bin/debug/CurryOn.Akka.Persistence.Elasticsearch.dll"

open Akka.Persistence.Elasticsearch
open CurryOn.Elastic
open FSharp.Control

typeof<PersistedEvent>.GetCustomAttributes(typeof<Nest.ElasticsearchTypeAttribute>, true)

let intOp = operation { return 3 }
let decOp = operation { return 3M }
let mixedOp = 
    operation {
        let! x = intOp
        let! y = decOp
        return sprintf "%d = %f" x y
    }