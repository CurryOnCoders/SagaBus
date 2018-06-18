namespace CurryOn.Elastic

open FSharp.Control
open FSharp.Quotations
open FSharp.Quotations.DerivedPatterns
open FSharp.Quotations.Patterns
open Nest
open System
open System.Threading.Tasks

module Elasticsearch =

    let rec private getExprValue<'a> (valueExpr: Expr) =
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

    let rec private getFieldName (fieldExpr: Expr) =
        match fieldExpr with
        | PropertyGet (_,property,_) -> property.Name
        | _ -> failwith "Expression Is Not a PropertyGet"

    let rec private parseExpression (expr: Expr) =
        match expr with
        | Lambda (_,expression) -> parseExpression expression
        | SpecificCall <@@ (>) @@> (_,_,expressions) -> 
            match Type.GetTypeCode(expressions.Head.Type) with
            | TypeCode.Byte | TypeCode.SByte | TypeCode.Int16 | TypeCode.Int32 | TypeCode.Int64 | TypeCode.UInt16 | TypeCode.UInt32 | TypeCode.UInt64 ->
                <@ {Field = getFieldName expressions.Head; Term = CurryOn.Elastic.IntegerRange <| {Minimum = Exclusive (getExprValue<int64> expressions.Tail.Head); Maximum = Unbounded}} @>
            | TypeCode.Single | TypeCode.Double | TypeCode.Decimal ->
                <@ {Field = getFieldName expressions.Head; Term = DecimalRange <| {Minimum = Exclusive (getExprValue<decimal> expressions.Tail.Head); Maximum = Unbounded}} @>
            | TypeCode.DateTime ->
                <@ {Field = getFieldName expressions.Head; Term = CurryOn.Elastic.DateRange <| {Minimum = Exclusive (getExprValue<DateTime> expressions.Tail.Head); Maximum = Unbounded}} @>
            | TypeCode.String ->
                <@ {Field = getFieldName expressions.Head; Term = TagRange <| {Minimum = Exclusive (getExprValue<string> expressions.Tail.Head); Maximum = Unbounded}} @>
            | other ->
                failwithf "Unsupported Type %s (TypeCode = %A) in Greater-Than Expression" expressions.Head.Type.Name other
        | _ -> failwith "Expression was not a Lambda or a Known Specific Call"
        
    let internal getClient (settings: ElasticSettings) =
        let connectionSettings = settings.GetConnectionSettings()
        ElasticClient(connectionSettings)
    
    /// Connects to the specified Elasticsearch Instance with the given settings and returns an IElasticClient
    let connect settings =
        let client = getClient settings
        { new CurryOn.Elastic.IElasticClient with
            member __.IndexExists<'index when 'index: not struct> () = Elastic.indexExists<'index> client
            member __.CreateIndex<'index when 'index: not struct> () = Elastic.createIndex<'index> client None
            member __.CreateIndex<'index when 'index: not struct> creationRequest = Elastic.createIndex<'index> client <| Some creationRequest
            member __.CreateIndexIfNotExists<'index when 'index: not struct> () = Elastic.createIndexIfNotExists<'index> client None
            member __.DeleteIndex<'index when 'index: not struct> () = Elastic.deleteIndex<'index> client
            member __.RecreateIndex<'index when 'index: not struct> () = Elastic.recreateIndex<'index> client
            member __.DeleteOldDocuments<'index when 'index: not struct> date = Elastic.deleteOldDocuments<'index> client date
            member __.Index<'index when 'index: not struct> document = Elastic.index<'index> client document
            member __.BulkIndex<'index when 'index: not struct and 'index: equality> (requests: CurryOn.Elastic.IndexRequest<'index> seq) = Elastic.bulkIndex<'index> client requests
            member __.BulkIndex<'index when 'index: not struct> (documents: 'index seq) = Elastic.bulkIndexDocuments<'index> client documents
            member __.Get<'index when 'index: not struct> request = Elastic.get<'index> client request
            member __.Delete<'index when 'index: not struct> request = Elastic.delete<'index> client request
            member __.DeleteByQuery<'index when 'index: not struct> query = Elastic.deleteByQuery<'index> client query
            member __.Update<'index when 'index: not struct> request = Elastic.update<'index> client request
            member __.Search<'index when 'index: not struct> request = Elastic.search<'index> client request
            member __.Count<'index when 'index: not struct> () = Elastic.count<'index> client
            member __.Distinct<'index,'field when 'index: not struct> request = Elastic.distinctValues<'index,'field> client request
            member __.Dispose () =
                client.ConnectionSettings.Dispose()
        }
        
    /// Checks to see if an index for the given type already exists in Elasticsearch
    let indexExists<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) =
        client.IndexExists<'index> ()

    /// Creates an index for the given type in the Elasticsearch repository
    let createIndex<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) =
        client.CreateIndex<'index> ()

    /// Creates an index for the given type with the specified parameters in the Elasticsearch repository
    let createIndexWithParameters<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) request =
        client.CreateIndex<'index> request

    /// Deletes the the index for the given type and all documents contained therein from Elasticsearch
    let deleteIndex<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) =
        client.DeleteIndex<'index> ()

    /// Deletes and Recreates the index for the given type in Elasticsearch
    let recreateIndex<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) = 
        client.RecreateIndex<'index> ()

    /// Deletes all documents in the index for the given type older than the specified date
    let deleteOldDocuments<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) date =
        client.DeleteOldDocuments<'index> date

    /// Indexes the given document, adding it to Elasticsearch
    let index<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) document =
        client.Index<'index> document

    /// Bulk-Indexes all given documents, adding them to Elasticsearch using the specified IDs and metadata
    let bulkIndex<'index when 'index: not struct and 'index: equality> (client: CurryOn.Elastic.IElasticClient) (requests: CurryOn.Elastic.IndexRequest<'index> seq) =
        client.BulkIndex<'index> requests

    /// Bulk-Indxes all given documents, adding them to Elasticsearch with dynamically generated IDs and metadata
    let bulkIndexDocuments<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) (documents: 'index seq) =
        client.BulkIndex<'index> documents

    /// Retrieves the document with the specified ID from the Elasticsearch index
    let get<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) request =
        client.Get<'index> request

    /// Deletes the specified document from the Elasticsearch index
    let delete<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) request =
        client.Delete<'index> request

    /// Updates the specified document in the Elasticsearch index, performing either a scripted update, field-level updates, or an Upsert
    let update<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) request =
        client.Update<'index> request

    /// Executes a search against the specified Elasticsearch index
    let search<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) request =
        client.Search<'index> request

    /// Deletes all documents in the index that match the given query
    let deleteByQuery<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) query =
        client.DeleteByQuery<'index> query


    /// Executes a search applying the given filter expression to the Elasitcsearch index
    //let filter<'index when 'index: not struct> (client: CurryOn.Elastic.IElasticClient) (filterExpression: Expr<'index -> bool>) =
    //    let searchTerm =
    //        match filterExpression with
    //        |

module SearchResult =
    exception ElasticsearchEventsException of ElasticsearchEvent []
    
    let inline failed<'a> (result: OperationResult<'a,ElasticsearchEvent>) =
        result.Events |> List.toArray |> ElasticsearchEventsException

    let inline failedTask<'a> result =
        result |> failed<'a> |> Task.FromException<'a>

    let inline toTask<'a> result =
        match result with
        | Success success -> success.Result |> Task.FromResult<'a>
        | Failure _ -> result |> failedTask<'a>


module SearchOperation =
    let inline toTask<'a> (operation: Operation<'a, ElasticsearchEvent>) =
        async {
            let! result = operation |> Operation.waitAsync
            return result |> SearchResult.toTask
        } |> Async.StartAsTask |> fun task -> task.Unwrap()