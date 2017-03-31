namespace CurryOn.Core

open Akka
open Akka.FSharp
open Akka.Routing
open CurryOn.Common

module Agent =
    
    type MessageProcessingAgent () =
        static let akka = System.create "sagaBus" <| Akka.FSharp.Configuration.load()
        let routerConfig = ConsistentHashingPool(Configuration.Bus.Agents.NumberOfSharedAgents)
        let router = routerConfig.CreateRouter akka
        let processMessage (message: IMessage) =
            tryAsync {
                return ()
            }
        interface IMessageProcessingAgent with
            member this.ProcessMessage message = processMessage message

