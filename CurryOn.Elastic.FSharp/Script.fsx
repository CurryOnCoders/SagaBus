#r @"..\packages\Newtonsoft.Json.10.0.1\lib\net45\Newtonsoft.Json.dll"
#r @"..\packages\Elasticsearch.Net.5.6.0\lib\net46\Elasticsearch.Net.dll"
#r @"..\packages\NEST.5.6.0\lib\net46\Nest.dll"
#r @"..\packages\FSharp.Quotations.Evaluator.1.0.7\lib\net40\FSharp.Quotations.Evaluator.dll"
#r @"..\CurryOn.FSharp.Control\bin\Release\CurryOn.FSharp.Control.dll"
#load "Model.fs"

open FSharp.Quotations
open FSharp.Quotations.DerivedPatterns
open FSharp.Quotations.ExprShape
open FSharp.Quotations.Patterns
open System
open CurryOn.Elastic

type MyIndex = { A: int; B: string; C: DateTime }

let rec getExprValue<'a> (valueExpr: Expr) =
    match valueExpr with
    | Int16 i -> Convert.ChangeType(i, typeof<'a>)
    | Int32 i -> Convert.ChangeType(i, typeof<'a>)
    | Int64 i -> Convert.ChangeType(i, typeof<'a>)
    | Single f -> Convert.ChangeType(f, typeof<'a>)
    | Double f -> Convert.ChangeType(f, typeof<'a>)
    | Decimal f -> Convert.ChangeType(f, typeof<'a>)
    | String s -> Convert.ChangeType(s, typeof<'a>)
    | Patterns.Value (value, valueType) ->
        if Type.GetTypeCode(valueType) = Type.GetTypeCode(typeof<'a>)
        then value 
        else failwithf "Unsupport Value Expression Type: %s" valueExpr.Type.Name
    | _ -> failwith "Expression was not a Value expression"
    |> unbox<'a>

let rec parseExpression (expr: Expr) =
    let rec getFieldName (fieldExpr: Expr) =
        match fieldExpr with
        | PropertyGet (_,property,_) -> property.Name
        | _ -> failwith "Expression Is Not a PropertyGet"

    match expr with
    | Lambda (_,expression) -> parseExpression expression
    | SpecificCall <@@ (>) @@> (_,_,expressions) -> 
        match Type.GetTypeCode(expressions.Head.Type) with
        | TypeCode.Byte | TypeCode.SByte | TypeCode.Int16 | TypeCode.Int32 | TypeCode.Int64 | TypeCode.UInt16 | TypeCode.UInt32 | TypeCode.UInt64 ->
            <@ {Field = getFieldName expressions.Head; Term = IntegerRange <| {Minimum = Exclusive (getExprValue<int64> expressions.Tail.Head); Maximum = Unbounded}} @>
        | TypeCode.Single | TypeCode.Double | TypeCode.Decimal ->
            <@ {Field = getFieldName expressions.Head; Term = DecimalRange <| {Minimum = Exclusive (getExprValue<decimal> expressions.Tail.Head); Maximum = Unbounded}} @>
        | TypeCode.DateTime ->
            <@ {Field = getFieldName expressions.Head; Term = DateRange <| {Minimum = Exclusive (getExprValue<DateTime> expressions.Tail.Head); Maximum = Unbounded}} @>
        | TypeCode.String ->
            <@ {Field = getFieldName expressions.Head; Term = TagRange <| {Minimum = Exclusive (getExprValue<string> expressions.Tail.Head); Maximum = Unbounded}} @>
        | other ->
            failwithf "Unsupported Type %s (TypeCode = %A) in Greater-Than Expression" expressions.Head.Type.Name other

let myFilter = <@ fun (a: MyIndex) -> a.A > 1 @>

myFilter |> parseExpression |> FSharp.Quotations.Evaluator.QuotationEvaluator.Evaluate

