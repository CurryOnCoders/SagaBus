namespace CurryOn.Common

open FSharp.Quotations
open Microsoft.FSharp.Linq.RuntimeHelpers
open System
open System.Linq


module Linq =
    let toLinq<'a,'b> (quote: Expr<'a -> 'b>) = 
        quote |> (fun q -> <@ new Func<'a,'b>(%q) @>) |> LeafExpressionConverter.QuotationToLambdaExpression

    let toLinqAction<'a> (quote: Expr<'a -> unit>) =
        quote |> (fun q -> <@ new Action<'a>(%q) @>) |> LeafExpressionConverter.QuotationToLambdaExpression


    // Partial Query Builder for composing LINQ queries
    type PartialQueryBuilder() =
        inherit Linq.QueryBuilder()

        member this.Run(e: Expr<Linq.QuerySource<'T, IQueryable>>) = e

    let pquery = PartialQueryBuilder()

    type Linq.QueryBuilder with
        [<ReflectedDefinition>]
        member this.Source(qs: Linq.QuerySource<'T, _>) = qs
    

