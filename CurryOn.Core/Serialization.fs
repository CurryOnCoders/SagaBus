namespace CurryOn.Core

open CurryOn.Common
open Newtonsoft.Json
open Newtonsoft.Json.Converters
open Newtonsoft.Json.Serialization
open System
open System.Text

module Serialization =
    let UTF8NoBOM = UTF8Encoding(false)
    
    type SerializationFormat = 
    | Xml 
    | Json
    | Wcf
    | Positional
    | Compressed of SerializationFormat   
    | Detect
    | Unsupported
    with
        member fmt.UnderlyingFormat =
            match fmt with
            | Xml -> Xml
            | Json -> Json
            | Wcf -> Wcf
            | Positional -> Positional
            | Compressed format ->
                match format with
                | Xml -> Xml
                | Json -> Json
                | Wcf -> Wcf
                | Positional -> Positional
                | Detect -> Detect
                | _-> Unsupported
            | Detect -> Detect
            | Unsupported -> Unsupported

    type ContractResolver() =
        inherit DefaultContractResolver()
        let camelCaseResolver = CamelCasePropertyNamesContractResolver()
        override this.ResolveContract (contractType) =
            if contractType.Assembly.FullName |> like "*SignalR*" 
            then base.ResolveContract(contractType)
            else camelCaseResolver.ResolveContract(contractType)

    [<AbstractClass>]
    type AttributeBasedConverter() =
        inherit JsonConverter()
        abstract member Attribute: Type
        member this.TryGetAttribute (objectType: Type) =
            objectType.GetCustomAttributes(true) |> Seq.tryFind (fun attribute -> attribute.GetType() = this.Attribute)
        override this.CanConvert (objectType: Type) =
            match this.TryGetAttribute objectType with
            | Some _ -> true
            | None -> false

    type JsonMeasureConverter() =
        inherit AttributeBasedConverter()
        let getValueProperty (objectType: Type) =
            objectType.GetMethods() |> Array.find (fun m -> m.Name.Equals("get_Value"))
        override __.Attribute = typeof<TransparentMeasureAttribute>
        member this.GetMeasureAttribute objectType =
            match this.TryGetAttribute objectType with
            | Some attribute -> attribute |> unbox<TransparentMeasureAttribute> |> Some
            | None -> None
        override __.WriteJson (writer, value, _) =
            let valueType = value.GetType()
            let getter = getValueProperty valueType
            getter.Invoke(value, [||]) |> writer.WriteValue 
        override this.ReadJson (reader, objectType, _, serializer) =
            match this.GetMeasureAttribute objectType with
            | Some measureAttribute -> 
                match measureAttribute.TypeCode with
                | TypeCode.Boolean ->
                    {bool.Value = reader.Value |> unbox<bool>} |> box
                | TypeCode.DateTime ->
                    {DateTime.Value = reader.Value |> unbox<DateTime>} |> box
                | TypeCode.String ->
                    {string.Value = reader.Value |> unbox<string>} |> box
                | TypeCode.Object ->
                    if objectType = typeof<Guid> 
                    then {Guid.Value = reader.Value |> unbox<Guid>} |> box
                    else {obj.Value = reader.Value} |> box
                | t -> failwithf "JSON MeasureConverter - Unsupported Type: %A" t
            | None -> serializer.Deserialize(reader, objectType)

    let JsonSettings = JsonSerializerSettings(
                            ContractResolver = ContractResolver(),
                            DateFormatHandling = DateFormatHandling.IsoDateFormat,
                            NullValueHandling = NullValueHandling.Ignore,
                            DefaultValueHandling = DefaultValueHandling.Ignore,
                            MissingMemberHandling = MissingMemberHandling.Ignore,
                            TypeNameHandling = TypeNameHandling.None,
                            Converters = [| StringEnumConverter(); JsonMeasureConverter(); |]
                        )