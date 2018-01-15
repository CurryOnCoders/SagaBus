namespace CurryOn.Elastic

open System
open FSharp.Reflection

[<AutoOpen>]
module internal Common =
    let inline toNullable<'a when 'a: struct and 'a :> ValueType and 'a: (new: unit -> 'a)> (opt: Option<'a>) = 
        match opt with
        | Some v -> Nullable v
        | None -> Nullable<'a>()

    let isNotNull<'a when 'a: null> = isNull<'a> >> not

    let getCaseName (x: 'a) = 
        match FSharpValue.GetUnionFields(x, typeof<'a>) with
        | case, _ -> case.Name  