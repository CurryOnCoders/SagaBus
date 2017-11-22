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
            let searchName =
                if name.EndsWith("[]")
                then name.Substring(0, name.Length - 2)
                else name
            let foundType =
                AppDomain.CurrentDomain.GetAssemblies()
                |> Seq.collect (fun assembly -> try assembly.GetTypes() with | _ -> [||])
                |> Seq.tryFind (fun clrType -> 
                    if searchName.Contains(".")
                    then let typeNamespace = searchName.Substring(0, searchName.LastIndexOf('.'))
                         let typeName = searchName.Substring(searchName.LastIndexOf('.') + 1)
                         clrType.Namespace = typeNamespace && clrType.Name = typeName
                    else clrType.Name = name)
            match foundType with
            | Some clrType -> 
                if searchName = name
                then clrType |> Success
                else clrType.MakeArrayType() |> Success
            | None -> Failure ex

