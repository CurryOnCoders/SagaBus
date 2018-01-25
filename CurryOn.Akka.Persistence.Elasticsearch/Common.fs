namespace CurryOn.Akka

open CurryOn.Elastic
open System
open System.Linq.Expressions

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<AutoOpen>]
module internal Common =
    let toProps<'actor> expr =
        let e = expr |> Linq.RuntimeHelpers.LeafExpressionConverter.QuotationToExpression
        let call = e :?> MethodCallExpression
        let lambda = call.Arguments.[0] :?> LambdaExpression
        Expression.Lambda<Func<'actor>>(lambda.Body)

    type DocumentId with
        member this.ToInt () =
            match this with
            | IntegerId i -> i
            | _ -> 0L

