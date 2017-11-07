namespace CurryOn.Common

open System

module Types =
    let getType name = 
        try let exactType = Type.GetType name
            if exactType |> isNotNull
            then exactType |> Success
            else Failure <| (exn <| sprintf "The result of Type.GetType(\"%s\") was null" name)
        with | ex -> Failure ex

    let findType (name: string) =
        match getType name with
        | Success clrType -> clrType |> Success
        | Failure ex ->
            let foundType =
                AppDomain.CurrentDomain.GetAssemblies()
                |> Seq.collect (fun assembly -> try assembly.GetTypes() with | _ -> [||])
                |> Seq.tryFind (fun clrType -> 
                    if name.Contains(".")
                    then let typeNamespace = name.Substring(0, name.LastIndexOf('.'))
                         let typeName = name.Substring(name.LastIndexOf('.') + 1)
                         clrType.Namespace = typeNamespace && clrType.Name = name
                    else clrType.Name = name)
            match foundType with
            | Some clrType -> clrType |> Success
            | None -> Failure ex

