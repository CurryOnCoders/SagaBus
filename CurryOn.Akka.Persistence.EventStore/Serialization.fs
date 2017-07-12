namespace Akka.Persistence.EventStore

open Akka.Actor
open Akka.Serialization
open CurryOn.Common
open EventStore.ClientAPI
open System

[<AbstractClass>]
type EventStoreSerializer (actorSystem) =
    inherit Serializer(actorSystem)
    abstract member ToEvent: 'a -> EventData
    abstract member FromEvent<'a> : RecordedEvent -> 'a


type EventStoreSerialization (serialization: Serialization) =
    new (actorSystem: ActorSystem) = EventStoreSerialization(actorSystem.Serialization)
    member __.Serialize data eventType =
        let serializer = serialization.FindSerializerFor data
        match serializer with
        | :? EventStoreSerializer as eventSerializer -> eventSerializer.ToEvent data
        | _ -> 
            let typeName = 
                match eventType with
                | Some eventTypeName -> eventTypeName
                | None -> data |> getTypeName
            EventData(Guid.NewGuid(), typeName, true, serializer.ToBinary(data), [||])
    member __.Deserialize<'a> (event: RecordedEvent) =
        let serializer = serialization.FindSerializerForType typeof<'a>
        match serializer with
        | :? EventStoreSerializer as eventSerializer -> eventSerializer.FromEvent<'a> event
        | _ -> serializer.FromBinary(event.Data, typeof<'a>) |> unbox<'a>