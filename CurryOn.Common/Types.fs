namespace CurryOn.Common

open FSharp.Control
open System

module Types =
    let getType name = 
        operation {
            let exactType = Type.GetType name
            if exactType |> isNotNull
            then return exactType
            else return! Failure [(exn <| sprintf "The result of Type.GetType(\"%s\") was null" name)]
        }

    let findType (name: string) =
        operation {
            let result = getType name |> Operation.wait
            return! 
                match result with
                | Success success -> success |> Success
                | Failure errors ->
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
                        then clrType |> Result.success
                        else clrType.MakeArrayType() |> Result.success
                    | None -> Failure errors
        }

