﻿namespace CurryOn.Elastic

open FSharp.Control
open FSharp.Quotations
open FSharp.Quotations.Patterns
open Nest

module FieldExpr =
    let rec getFieldName getField =
        match getField with
        | Lambda (var,expr) -> getFieldName expr
        | PropertyGet (_,property,_) ->
            match property.GetCustomAttributes(typeof<ElasticsearchPropertyAttributeBase>, true) with
            | [||] -> property.Name
            | attributes -> attributes |> Seq.head |> unbox<ElasticsearchPropertyAttributeBase> |> fun a -> a.Name
        | _ -> failwith "getField must be PropertyGet expression"

    let inline fieldContains getField value =
        let fieldName = getFieldName getField
        {Field = fieldName; Term = Contains value}

    let inline fieldRange getField range =
        let fieldName = getFieldName getField
        {Field = fieldName; Term = range}

module Sort =
    /// Sort by the specified field in the given direction
    let inline field<'index, 'field when 'index: not struct> direction (getField: Expr<'index -> 'field>) =
        FieldDirection (FieldExpr.getFieldName getField, direction)

    /// Sort by the specified field ascending
    let inline ascending<'index, 'field when 'index: not struct> getField =
        field<'index,'field> Ascending getField

    /// Sort by the specified field descending
    let inline descending<'index, 'field when 'index: not struct> getField =
        field<'index,'field> Descending getField

module Search =
    /// Create an Elasitcsearch Search Request from the provided Search Structure and other parameters
    let inline build timeout sort from size search =
        {Search = search; Timeout = timeout; Sort = sort; From = from; Size = size}

    /// Execute the Search on the provided Elasticsearch Client with the specified parameters
    let inline execute<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) timeout sort from size search =
        search |> build timeout sort from size |> client.Search<'index>

    /// Execute a DeleteByQuery for the given query on the provided Elasticsearch Client
    let inline delete<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) timeout sort from size search =
        search |> build timeout sort from size |> client.DeleteByQuery<'index>
            
    /// Select the Top n documents matching the given query ordered by the given sort
    let inline top<'index when 'index: not struct> client timeout sort n search =
        search |> execute<'index> client timeout (Some sort) None (Some n)

    /// Select the Top 1 document matching the given query ordered by the given sort
    let inline first<'index when 'index: not struct> client timeout sort search = 
        operation {
            let! topResult = search |> top<'index> client timeout sort 1
            return! topResult.Results.Hits |> List.map (fun hit -> hit.Document) |> List.head |> Result.success
        }

module Query =    
    /// Search for a specific field that contains the given value
    let inline field<'index,'field when 'index: not struct> (getField: Expr<'index -> 'field>) (value: 'field) =
        FieldValue <| FieldExpr.fieldContains getField (value.ToString())

    /// Search for a field with a value in the specified integer range
    let inline range<'index,'field when 'index: not struct> (getField: Expr<'index -> 'field>) (min: int64 RangeTerm) (max: int64 RangeTerm) =
        FieldValue <| FieldExpr.fieldRange getField (CurryOn.Elastic.IntegerRange <| {Minimum = min; Maximum = max})

    /// Combine the given Query terms using the provided Operator (AND/OR)
    let inline combine operator query1 query2 =
        match operator with
        | And -> QueryAnd (query1, query2)
        | Or -> QueryOr (query1, query2)
        
    /// Create an Elasticsearch Query String Query from the provided term
    let inline build term =
        CurryOn.Elastic.QueryStringQuery <| {Term = term; DefaultField = None; DefaultOperator = None}

    /// Execute a Query built from the given Term on the provided Elasticsearch Client with the specified parameters
    let inline execute<'index when 'index: not struct> client timeout sort from size term = 
        term |> build |> Search.execute<'index> client timeout sort from size

    /// Execute a DeleteByQuery built from the for the given Term on the provided Elasticsearch Client
    let inline delete<'index when 'index: not struct> client timeout sort from size term =
        term |> build |> Search.delete<'index> client timeout sort from size

    /// Select the Top 1 document matching the given query ordered by the given sort
    let inline first<'index when 'index: not struct> client timeout sort query = 
        query |> build |> Search.first<'index> client timeout sort