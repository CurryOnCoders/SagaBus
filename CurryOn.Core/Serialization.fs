namespace CurryOn.Core

open CurryOn.Common
open MBrace.FsPickler
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

   