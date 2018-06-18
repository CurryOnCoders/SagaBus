namespace CurryOn.Elastic

open FSharp.Control
open FSharp.Quotations
open FSharp.Quotations.Patterns
open Nest
open System

module FieldExpr =
    let rec getFieldName getField =
        match getField with
        | Lambda (var,expr) -> getFieldName expr
        | PropertyGet (_,property,_) ->
            match property.GetCustomAttributes(typeof<ElasticsearchPropertyAttributeBase>, true) with
            | [||] -> property.Name
            | attributes -> 
                match attributes |> Seq.head with 
                | :? ExactMatchAttribute as exact -> sprintf "%s.keyword" exact.Name
                | :? ElasticsearchPropertyAttributeBase as attribute -> attribute.Name
                | _ -> failwithf "Property '%s' must have an ElasticsearchProperty attribute" property.Name
        | _ -> failwith "getField must be PropertyGet expression"

    let inline fieldContains getField value =
        let fieldName = getFieldName getField
        {Field = fieldName; Term = Contains value}

    let inline fieldRange getField range =
        let fieldName = getFieldName getField
        {Field = fieldName; Term = range}

    let inline fieldTerm getField term boost =
        let fieldName = getFieldName getField
        {Field = fieldName; Value = term; Boost = boost}

    let inline fieldTerms getField terms =
        let fieldName = getFieldName getField
        {Field = fieldName; Values = terms |> Seq.map box |> Seq.toList}

    let inline fieldMatch getField query operator zeroTerms =
        let fieldName = getFieldName getField
        {Field = fieldName; Query = query |> box; Operator = operator; ZeroTerms = zeroTerms}

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
        search |> execute<'index> client timeout sort None (Some n)

    /// Select the Top 1 document matching the given query ordered by the given sort
    let inline private head<'index when 'index: not struct> client timeout sort search (headOp: 'index list -> 'index option) = 
        operation {
            let! topResult = search |> top<'index> client timeout sort 1
            return! 
                if true
                then topResult.Results.Hits |> List.map (fun hit -> hit.Document) |> headOp |> Result.success
                else Result.failure [UnhandledException (InvalidOperationException() :> exn)]
        }

    /// Select the Top 1 document matching the given query ordered by the given sort
    let inline first<'index when 'index: not struct> client timeout sort search = 
       List.tryHead |> head<'index> client timeout (Some sort) search 

    /// Take the only document matching the given query from the index.
    /// If more than one document matches the query, the result is None
    let inline single<'index when 'index: not struct> client timeout search =
        (fun hits -> try hits |> List.exactlyOne |> Some with | _ -> None) |> head<'index> client timeout None search

    /// Select the Distinct Values and their Document Counts for the given field in the Elasticsearch Index
    let inline distinct<'index,'field when 'index: not struct> (getField: Expr<'index -> 'field>) size (client: CurryOn.Elastic.IElasticClient) =
        client.Distinct<'index,'field>({Field = FieldExpr.getFieldName getField; Size = size})

module Query =    
    /// Search for a specific field that contains the given value
    let inline field<'index,'field when 'index: not struct> (getField: Expr<'index -> 'field>) (value: 'field) =
        FieldValue <| FieldExpr.fieldContains getField (value.ToString())

    /// Search for a field with a value in the specified integer range
    let inline range<'index,'field when 'index: not struct> (getField: Expr<'index -> 'field>) (min: int64 RangeTerm) (max: int64 RangeTerm) =
        FieldValue <| FieldExpr.fieldRange getField (CurryOn.Elastic.IntegerRange <| {Minimum = min; Maximum = max})

    /// Search for a field with a value in the specified integer range
    let inline dateRange<'index,'field when 'index: not struct> (getField: Expr<'index -> 'field>) (min: DateTime RangeTerm) (max: DateTime RangeTerm) =
        FieldValue <| FieldExpr.fieldRange getField (CurryOn.Elastic.DateRange <| {Minimum = min; Maximum = max})

    /// Combine the given Query terms using the provided Operator (AND/OR)
    let inline combine operator query1 query2 =
        match operator with
        | And -> QueryAnd (query1, query2)
        | Or -> QueryOr (query1, query2)

    /// Combine the given Query terms using the AND operator
    /// Note: The keyword 'and' is reserved by F#, so this function must start with an uppercase letter
    let And = combine And
        
    /// Combine the given Query terms using the OR operator
    /// Note: The keyword 'or' is reserved by F#, so this function must start with an uppercase letter
    let Or = combine Or
        
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

    /// Take the only document matching the given query from the index.
    /// If more than one document matches the query, the result is None
    let inline single<'index when 'index: not struct> client timeout query = 
        query |> build |> Search.single<'index> client timeout

module Dsl =
    /// Create an Elasticsearch Query DSL 'Term' Query Clause
    let inline term<'index,'field when 'index: not struct> (getField: Expr<'index -> 'field>) (searchTerm: 'field) boost =
        Term <| FieldExpr.fieldTerm getField searchTerm boost

    /// Create an Elasticsearch Query DSL 'Terms' Query Clause
    let inline terms<'index,'field when 'index: not struct> (getField: Expr<'index -> 'field []>) (searchTerms: 'field seq) =
        Terms <| FieldExpr.fieldTerms getField searchTerms

    /// Create an Elasticsearch Query DSL 'Match' Query Clause
    /// Note: The keyword 'match' is reserved by F#, so this function must start with an uppercase letter.
    let inline Match<'index,'field when 'index: not struct> (getField: Expr<'index -> 'field>) (query: 'field) operator zeroTerms =
        Match <| FieldExpr.fieldMatch getField query operator zeroTerms

    /// Create an Elasticsearch Query DSL 'MatchAll' Query Clause
    let inline matchAll<'index when 'index: not struct> boost = MatchAll boost

    /// Create an Elasticsearch Query DSL 'MatchNone' Query Clause
    let inline matchNone<'index when 'index: not struct> () = MatchNone

    /// Create an Elasticsearch Query DSL 'Range' Query Clause for a numeric value range
    let inline range<'index,'field when 'index: not struct and 'field :> IConvertible and 'field :> IComparable> (getField: Expr<'index -> 'field>) (lower: 'field LowerRange) (upper: 'field UpperRange) boost =
        let lowerRange = 
            match lower with
            | UnboundedLower -> UnboundedLower
            | GreaterThanOrEqual n -> GreaterThanOrEqual (n |> Convert.ToDouble)
            | GreaterThan n -> GreaterThan (n |> Convert.ToDouble)
        let upperRange = 
            match upper with
            | UnboundedUpper -> UnboundedUpper
            | LessThanOrEqual n -> LessThanOrEqual (n |> Convert.ToDouble)
            | LessThan n -> LessThan (n |> Convert.ToDouble)
        CurryOn.Elastic.NumericRangeQuery <| {Field = FieldExpr.getFieldName getField; LowerRange = lowerRange; UpperRange = upperRange; Boost = boost; Format = None; TimeZone = None}

    /// Create an Elasticsearch Query DSL 'Range' Query Clause for a Date range
    let inline dateRange<'index when 'index: not struct> (getField: Expr<'index -> DateTime>) (lower: DateTime LowerRange) (upper: DateTime UpperRange) boost format timeZone =
        CurryOn.Elastic.DateRangeQuery <| {Field = FieldExpr.getFieldName getField; LowerRange = lower; UpperRange = upper; Boost = boost; Format = format; TimeZone = timeZone}

    /// Combine the given Query DSL clauses into a Bool Query DSL clause
    let inline bool should filter mustNot must =
        Bool {Must = must; Should = should; Filter = filter; MustNot = mustNot}
     
    /// Create an Elasticsearch RequestBody Query from the provided Query DSL Clause
    let inline build clause =
        RequestBodyQuery clause

    /// Execute a Query built from the given Query DSL clause on the provided Elasticsearch Client with the specified parameters
    let inline execute<'index when 'index: not struct> client timeout sort from size clause =
        clause |> build |> Search.execute<'index> client timeout sort from size

    /// Execute a DeleteByQuery built from the for the given Query DSL clause on the provided Elasticsearch Client
    let inline delete<'index when 'index: not struct> client timeout sort from size clause =
        clause |> build |> Search.delete<'index> client timeout sort from size

    /// Select the Top 1 document matching the given Query DSL clause ordered by the given sort
    let inline first<'index when 'index: not struct> client timeout sort clause = 
        clause |> build |> Search.first<'index> client timeout sort

    /// Take the only document matching the given query from the index.
    /// If more than one document matches the query, the result is None
    let inline single<'index when 'index: not struct> client timeout clause = 
        clause |> build |> Search.single<'index> client timeout