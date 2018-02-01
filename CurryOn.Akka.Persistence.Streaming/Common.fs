namespace CurryOn.Akka

open CurryOn.Common
open System
open System.Linq.Expressions

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<AutoOpen>]
module Common =
    let toProps<'actor> expr =
        let e = expr |> Linq.RuntimeHelpers.LeafExpressionConverter.QuotationToExpression
        let call = e :?> MethodCallExpression
        let lambda = call.Arguments.[0] :?> LambdaExpression
        Expression.Lambda<Func<'actor>>(lambda.Body)

    let searchForType = memoize <| Types.findType       