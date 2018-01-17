namespace CurryOn.Elastic

open FSharp.Quotations
open FSharp.Quotations.Patterns
open Nest

module internal Core =
    let private getFieldName getField =
        match getField with
        | PropertyGet (_,property,_) ->
            match property.GetCustomAttributes(typeof<ElasticsearchPropertyAttributeBase>, true) with
            | [||] -> property.Name
            | attributes -> attributes |> Seq.head |> unbox<ElasticsearchPropertyAttributeBase> |> fun a -> a.Name
        | _ -> failwith "getField must be PropertyGet expression"

    let fieldContains getField value =
        let fieldName = getFieldName getField
        {Field = fieldName; Term = Contains value}

    let fieldRange getField range =
        let fieldName = getFieldName getField
        {Field = fieldName; Term = range}

module Query =    
    /// Search for a specific field that contains the given value
    let field<'index,'field when 'index: not struct> (getField: Expr<'index -> 'field>) (value: 'field) =
        FieldValue <| Core.fieldContains getField (value.ToString())

    /// Search for a field with a value in the specified integer range
    let range<'index,'field when 'index: not struct> (getField: Expr<'index -> 'field>) (min: int64 RangeTerm) (max: int64 RangeTerm) =
        FieldValue <| Core.fieldRange getField (CurryOn.Elastic.IntegerRange <| {Minimum = min; Maximum = max})

    /// Combine the given Query terms using the provided Operator (AND/OR)
    let combine operator query1 query2 =
        match operator with
        | And -> QueryAnd (query1, query2)
        | Or -> QueryOr (query1, query2)

    /// Create an Elasticsearch Query String Query from the provided term
    let build term =
        CurryOn.Elastic.QueryStringQuery <| {Term = term; DefaultField = None; DefaultOperator = None}

module Search =
    /// Create an Elasitcsearch Search Request from the provided Search Structure and other parameters
    let build timeout sort from size structure =
        {Search = structure; Timeout = timeout; Sort = sort; From = from; Size = size}