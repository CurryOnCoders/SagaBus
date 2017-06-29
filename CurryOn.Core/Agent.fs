namespace CurryOn.Core

open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Persistence
open CurryOn.Common
open System
open System.Reflection

[<CLIMutable>]
type internal SaveSnapshot<'a when 'a :> IAggregate> =
    {
        State: 'a
    } interface ICommand

[<AbstractClass>]
type AggregateActorBase<'a when 'a :> IAggregate> (aggregateKey: string) as actor=
    inherit ReceivePersistentActor()
    let context = ActorBase.Context
    let aggregateType = typeof<'a>
    let lastSnapshotTime = ref DateTime.MinValue
    let initialState =
        let emptyProperty = aggregateType.GetProperty("Empty", BindingFlags.Public ||| BindingFlags.Static)
        if emptyProperty |> isNull
        then let emptyMethod = aggregateType.GetMethod("get_Empty", BindingFlags.Public ||| BindingFlags.Static)
             if emptyMethod |> isNull
             then Unchecked.defaultof<'a>
             else emptyMethod.Invoke(null, [||]) |> unbox<'a>
        else emptyProperty.GetValue(null) |> unbox<'a>
    let applyEvent, recoverSnapshot, getCurrentState =
        let state = ref initialState
        (fun event -> 
            let currentState = state.Value
            let newState = currentState.Apply event (currentState.LastEventNumber + 1L) |> unbox<'a>
            state := newState
            match currentState.SnapshotPolicy with
            | EveryEvent -> context.Self <! {State = newState}
            | EventCount count -> if newState.LastEventNumber % count = 0L
                                  then context.Self <! {State = newState}
            | TimeBased timeSpan -> if DateTime.Now - lastSnapshotTime.Value > timeSpan
                                    then context.Self <! {State = newState}),
        (fun snapshot -> state := snapshot),
        (fun () -> state.Value)

    do 
        actor.Command<SaveSnapshot<'a>>(Action<SaveSnapshot<'a>>(fun command -> 
            actor.SaveSnapshot(command.State)
            lastSnapshotTime := DateTime.Now))
        actor.Command<SaveSnapshotSuccess>(Action<SaveSnapshotSuccess>(fun command ->
            let upperBound = command.Metadata.SequenceNr - 1L
            actor.DeleteMessages(upperBound)
            actor.DeleteSnapshots(SnapshotSelectionCriteria(upperBound))))
        actor.Recover<SnapshotOffer>(Action<SnapshotOffer>(fun snapshotOffer -> 
            lastSnapshotTime := snapshotOffer.Metadata.Timestamp
            snapshotOffer.Snapshot |> unbox<'a> |> recoverSnapshot))
        actor.Recover<IMessage>(Action<IMessage>(fun message ->
            match message.Body with
            | :? IEvent as event -> applyEvent event
            | _ -> ())) // ignore commands when recovering state


    override this.PersistenceId = sprintf "%s-%s" aggregateType.Name aggregateKey
    member __.ApplyEvent event = applyEvent event
    member __.GetCurrentState () = getCurrentState ()
