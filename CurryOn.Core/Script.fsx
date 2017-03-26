#r "System.Configuration"
#r "System.Runtime.Serialization"
#r "System.Xml"
#r @"C:\Projects\GitHub\CurryOn\CurryOn\packages\Akka.1.1.3\lib\net45\Akka.dll"
#r @"C:\Projects\GitHub\CurryOn\CurryOn\packages\Akka.FSharp.1.1.3\lib\net45\Akka.FSharp.dll"
#r @"C:\Projects\GitHub\CurryOn\CurryOn\packages\FSPowerPack.Core.Community.3.0.0.0\lib\Net40\FSharp.PowerPack.dll"
#r @"C:\Projects\GitHub\CurryOn\CurryOn\packages\FSPowerPack.Linq.Community.3.0.0.0\lib\Net40\FSharp.PowerPack.Linq.dll"
#r @"C:\Projects\GitHub\CurryOn\CurryOn\packages\FsPickler.1.2.21\lib\net45\FsPickler.dll"
#r @"C:\Projects\GitHub\CurryOn\CurryOn\packages\Newtonsoft.Json.9.0.1\lib\net45\Newtonsoft.Json.dll"
#r @"C:\Projects\GitHub\CurryOn\CurryOn\packages\System.Collections.Immutable.1.1.36\lib\portable-net45+win8+wp8+wpa81\System.Collections.Immutable.dll"
#r @"..\packages\FSharp.Quotations.Evaluator.1.0.7\lib\net40\FSharp.Quotations.Evaluator.dll"
#r @"..\CurryOn.Common\bin\debug\CurryOn.Common.dll"
#load "Messaging.fs"

open CurryOn.Common
open CurryOn.Core

type EventHub () =
    let publish (x: IEvent<_>) = Success () |> Async.Result
    interface IEventHub with
        member  __.PersistEvent event = publish event