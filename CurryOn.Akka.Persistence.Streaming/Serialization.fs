namespace CurryOn.Akka

open CurryOn.Common
open CurryOn
open MBrace.FsPickler
open MBrace.FsPickler.Json
open System
open System.IO
open System.Reflection

module Serialization =
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

    let parseJsonAs =
        let deserializer = JsonSerializer.GetType().GetMethods(BindingFlags.Public ||| BindingFlags.Instance) |> Seq.find (fun m -> m.Name = "Deserialize")
        fun (json: string) (clrType: Type) ->
            use reader = new StringReader(json)
            let method = deserializer.MakeGenericMethod(clrType)
            method.Invoke(JsonSerializer, [|reader; null; null; (Some false)|])

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