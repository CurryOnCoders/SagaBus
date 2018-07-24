namespace CurryOn.Core

open CurryOn.Common
open FSharp.Control
open MBrace.FsPickler
open MBrace.FsPickler.Json
open System
open System.IO
open System.Text

module Serialization =
    /// Cross-Platform UTF-8 Serialization (No BOM)
    let Utf8 = UTF8Encoding(false)

    // FsPickler Serializers
    let private JsonSerializer = JsonSerializer(indent = true, omitHeader = true)
    let private BinarySerializer = BinarySerializer(forceLittleEndian = true)
    let private BsonSerializer = BsonSerializer()
    let private XmlSerializer = XmlSerializer(indent = true)    

    let private toFormatBytes formatter =
        use stream = new MemoryStream()
        formatter stream
        stream.ToArray()

    let private toFormat formatter = toFormatBytes formatter |> Utf8.GetString

    let private serializeJson any (stream: Stream) = JsonSerializer.Serialize(stream, any)

    /// Convert any object to a JSON string
    let toJson any = serializeJson any |> toFormat 

    /// Convert any object to a JSON byte array
    let toJsonBytes any = serializeJson any |> toFormatBytes
    
    /// Convert a JSON string to an instance of the type parameter
    let parseJson<'t> json = 
        use reader = new StringReader(json)
        JsonSerializer.Deserialize<'t> reader

    /// Convert a JSON byte array to an instance of the type parameter
    let parseJsonBytes<'t> (bytes: byte[]) =
        use stream = new MemoryStream(bytes)
        JsonSerializer.Deserialize<'t> stream

    /// Convert any object to binary
    let toBinary any = (fun (stream: MemoryStream) -> BinarySerializer.Serialize(stream, any)) |> toFormatBytes

    /// Convert a binary serialized object to an instance of the type parameter
    let parseBinary<'t> (binary: byte[]) =
        use stream = new MemoryStream(binary)
        BinarySerializer.Deserialize<'t> stream

    /// Convert any object to BSON
    let toBson any = (fun (stream: MemoryStream) -> BsonSerializer.Serialize(stream, any)) |> toFormatBytes 

    /// Convert a BSON byte array to an instance of the type parameter
    let parseBson<'t> (bson: byte[]) =
        use stream = new MemoryStream(bson)
        BsonSerializer.Deserialize<'t> stream
    
    let private serializeXml any (stream: Stream) = XmlSerializer.Serialize(stream, any)

    /// Convert any object to an XML string
    let toXml any = serializeXml any |> toFormat

    /// Convert any object to an XML byte array
    let toXmlBytes any = serializeXml any |> toFormatBytes

    /// Convert an XML byte array to an instance of the type parameter
    let parseXmlBytes<'t> (bytes: byte[]) =
        use stream = new MemoryStream(bytes)
        XmlSerializer.Deserialize<'t> stream

    /// Convert an XML string to an instance of the type parameter
    let parseXml<'t> (xml: string) =
        xml |> Utf8.GetBytes |> parseXmlBytes<'t>    

    /// Serialize any object to a string of the specified format
    let serialize format any =
        operation {
            let serializer =
                match format with
                | Xml -> toXml
                | Json -> toJson            
                | Bson -> toBson >> Convert.ToBase64String
                | Binary -> toBinary >> Convert.ToBase64String
                | other -> failwithf "The serialization format '%A' is not yet implmented" other
            return any |> serializer
        }

    /// Serialize any object to a byte array of the specified format
    let serializeToBytes format any =
        operation {
            let serializer =
                match format with
                | Xml -> toXmlBytes
                | Json -> toJsonBytes            
                | Bson -> toBson
                | Binary -> toBinary
                | other -> failwithf "The serialization format '%A' is not yet implmented" other
            return any |> serializer
        }

    /// Deserialize a string of the specified format to an instance of the type parameter
    let deserialize<'t> format str =
        operation {
            let deserializer =
                match format with
                | Xml -> parseXml<'t>
                | Json -> parseJson<'t>          
                | Bson -> Convert.FromBase64String >> parseBson<'t>
                | Binary -> Convert.FromBase64String >> parseBinary<'t> 
                | other -> failwithf "The serialization format '%A' is not yet implmented" other
            return str |> deserializer
        }

    /// Deserialize a byte array of the specified format to an instance of the type parameter
    let deserializeBytes<'t> format bytes =
        operation {
            let deserializer =
                match format with
                | Xml -> parseXmlBytes<'t>
                | Json -> parseJsonBytes<'t>         
                | Bson -> parseBson<'t>
                | Binary -> parseBinary<'t>
                | other -> failwithf "The serialization format '%A' is not yet implmented" other
            return bytes |> deserializer
        }