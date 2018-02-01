namespace Akka.Persistence.EventStore

open Akka.Actor
open Akka.Serialization
open CurryOn.Akka
open CurryOn.Common
open EventStore.ClientAPI
open System

[<CLIMutable>]
type EventMetadata =
    {
        EventType: string
        Sender: IActorRef
        Tags: string []
        Size: int
    }

type IEventStoreSerializer =
    abstract member ToEvent: 'a -> EventMetadata -> EventData
    abstract member FromEvent<'a> : RecordedEvent -> 'a

type EventStoreSerializer (actorSystem) =
    inherit Serializer(actorSystem)
    override __.ToBinary (o: obj) = o |> Serialization.toJsonBytes
    override __.FromBinary (bytes: byte[], clrType: Type) = Serialization.parseJsonBytesAs bytes clrType
    override __.IncludeManifest = false
    member __.FromBinary<'t> (bytes: byte[]) = bytes |> Serialization.parseJsonBytes<'t>     
    member this.ToEvent<'t> (any: 't) (metadata: EventMetadata) = 
        EventData(Guid.NewGuid(), typeof<'t>.FullName, true, this.ToBinary(any), metadata |> Serialization.toJsonBytes)
    member this.FromEvent<'t> (event: RecordedEvent) =
        event.Data |> this.FromBinary<'t>
    interface IEventStoreSerializer with
        member this.ToEvent<'a> any metadata = this.ToEvent<'a> any metadata
        member this.FromEvent<'a> event = this.FromEvent<'a> event


type EventStoreSerialization (serialization: Serialization) =
    new (actorSystem: ActorSystem) = EventStoreSerialization(actorSystem.Serialization)
    member __.Serialize data eventType metadata =
        let serializer = serialization.FindSerializerFor data
        match serializer with
        | :? EventStoreSerializer as eventSerializer -> eventSerializer.ToEvent data metadata
        | _ -> 
            let typeName = 
                match eventType with
                | Some eventTypeName -> eventTypeName
                | None -> data |> getTypeName
            EventData(Guid.NewGuid(), typeName, true, data |> Serialization.toJsonBytes, metadata |> Serialization.toJsonBytes)
    member __.Deserialize<'a> (event: RecordedEvent) =
        let serializer = serialization.FindSerializerForType typeof<'a>
        match serializer with
        | :? EventStoreSerializer as eventSerializer -> eventSerializer.FromEvent<'a> event
        | _ -> event.Data |> Serialization.parseJsonBytes<'a>