namespace CurryOn.Elastic.FSharp.Test

open CurryOn.Elastic
open Expecto
open FSharp.Control
open System

module DslTests =
    type SalesOrder = {Id: int}

    let testFirst =
        test "Test Dsl.first" {
            let settings = {Node = Uri "http://localhost:9200"; DisableDirectStreaming = true; RequestTimeout = TimeSpan.FromMinutes 1.0; DefaultIndex = Some "sales_orders"; IndexMappings = [{IndexName = "sales_orders"; TypeName = "sales_order"; Type = typeof<SalesOrder>}]}
            let client = Elasticsearch.connect settings 
            let result = (MatchAll None) |> Dsl.first client None (FieldDirection ("Id",Descending)) |> Operation.returnOrFail
            match result with
            | Some value -> Expect.isGreaterThan value.Id 0 "SalesOrder Id should be non-zero"
            | None -> failwith "No SalesOrder returned by Dsl.first"
        }