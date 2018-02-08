namespace CurryOn.Akka

open Akka.Actor
open Akka.Persistence

[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Messages =
    type SubscriberCommands =
        | SubscribeToAllPersistenceIds
        | SubscribeToPersistenceId of string        
        | SubscribeToTag of string

    type PersistenceIdSubscriptionCommands =
        | CurrentPersistenceIds of string list
        | PersistenceIdAdded of string

    type EventSubscriptionCommands =
        | Continue
        | EventAppended of string
        | TaggedEventAppended of string

    type ReplayCommands =
        | ReplayTaggedMessages of int64*int64*string*IActorRef

    type ReplayEvents =
        | ReplayedTaggedMessage of int64*string*IPersistentRepresentation 