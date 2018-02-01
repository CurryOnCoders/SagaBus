namespace CurryOn.Akka

open CurryOn.Elastic

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<AutoOpen>]
module internal Common =
    type DocumentId with
        member this.ToInt () =
            match this with
            | IntegerId i -> i
            | _ -> 0L

