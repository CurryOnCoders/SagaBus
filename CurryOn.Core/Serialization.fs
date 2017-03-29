namespace CurryOn.Core

open CurryOn.Common
open MBrace.FsPickler
open MBrace.FsPickler.Json
open System
open System.Text

module Serialization =
    let UTF8NoBOM = UTF8Encoding(false)
    let JsonSerializer = JsonSerializer(indent = true, omitHeader = true)
    
    
    
   