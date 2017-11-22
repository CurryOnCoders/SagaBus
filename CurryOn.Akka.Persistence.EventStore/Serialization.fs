namespace Akka.Persistence.EventStore

open Akka.Actor
open Akka.Serialization
open CurryOn.Common
open CurryOn
open EventStore.ClientAPI
open MBrace.FsPickler
open MBrace.FsPickler.Json
open System
open System.IO
open System.Reflection

module internal Serialization =
    let private BinarySerializer = BinarySerializer()
    let private JsonSerializer = JsonSerializer(indent = true)

    let toBinary any =
        use stream = new MemoryStream()
        BinarySerializer.Serialize(stream, any)
        stream.ToArray()

    let fromBinary<'t> (bytes: byte[]) =
        use stream = new MemoryStream(bytes)
        BinarySerializer.Deserialize<'t>(stream)

    let fromBinaryAs =
        let deserializer = BinarySerializer.GetType().GetMethods(BindingFlags.Public ||| BindingFlags.Instance) |> Seq.find (fun m -> m.Name = "Deserialize")
        fun (bytes: byte[]) (clrType: Type) ->
            use stream = new MemoryStream(bytes)
            let method = deserializer.MakeGenericMethod(clrType)
            method.Invoke(BinarySerializer, [|stream; null; null; null; (Some false)|])

    let toJson any = 
        use writer = new StringWriter()
        JsonSerializer.Serialize(writer, any)
        writer.ToString()

    let parseJson<'t> json =
        use reader = new StringReader(json)
        JsonSerializer.Deserialize<'t>(reader)

    let toJsonBytes any = 
        use stream = new MemoryStream()
        use writer = new StreamWriter(stream)
        JsonSerializer.Serialize(writer, any)
        stream.ToArray()

    let parseJsonBytes<'t> (bytes: byte[]) =
        use stream = new MemoryStream(bytes)
        use reader = new StreamReader(stream)
        JsonSerializer.Deserialize<'t>(reader)

    let parseJsonBytesAs =
        let deserializer = JsonSerializer.GetType().GetMethods(BindingFlags.Public ||| BindingFlags.Instance) |> Seq.find (fun m -> m.Name = "Deserialize")
        fun (bytes: byte[]) (clrType: Type) ->
            use stream = new MemoryStream(bytes)
            use reader = new StreamReader(stream)
            let method = deserializer.MakeGenericMethod(clrType)
            method.Invoke(JsonSerializer, [|reader; null; null; (Some false)|])

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