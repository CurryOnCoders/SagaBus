namespace CurryOn.Tests

open Akka.Actor
open Akka.Event
open Akka.FSharp

type DebugOutputLogger () as actor =
    inherit ReceiveActor ()
    let context = ActorBase.Context
    do 
        actor.Receive<Debug>            (fun e -> System.Diagnostics.Trace.WriteLine(e.ToString(), "DEBUG"))
        actor.Receive<Info>             (fun e -> System.Diagnostics.Trace.WriteLine(e.ToString(), "INFO"))
        actor.Receive<Warning>          (fun e -> System.Diagnostics.Trace.WriteLine(e.ToString(), "WARNING"))
        actor.Receive<Error>            (fun e -> System.Diagnostics.Trace.WriteLine(e.ToString(), "ERROR"))
        actor.Receive<InitializeLogger> (fun _ -> System.Diagnostics.Trace.WriteLine("Logger initialized", "INIT")
                                                  context.Sender <! Akka.Event.LoggerInitialized())