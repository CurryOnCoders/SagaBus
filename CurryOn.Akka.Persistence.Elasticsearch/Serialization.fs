namespace Akka.Persistence.Elasticsearch

open MBrace.FsPickler.Json
open System
open System.IO
open System.Reflection

module internal Serialization =
    let private JsonSerializer = JsonSerializer(indent = true)

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
