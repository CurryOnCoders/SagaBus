namespace CurryOn.Tests

open CurryOn.Elastic
open FSharp.Control
open Microsoft.VisualStudio.TestTools.UnitTesting
open System

[<CLIMutable>]
[<Indexed("sales_order", IdProperty = "id")>]
type SalesOrder =
    {
        [<Int64("id")>] Id: int64
        [<ExactMatch("message_id")>] MessageId: string
        [<Int64("event_sequence")>] EventSequence: int64
        [<FullText("system")>] System: string
        [<FullText("event_type")>] EventType: string
        [<FullText("action")>] Action: string
        [<Int64("order_number")>] OrderNumber: int64
        [<Int64("invoice_number")>] InvoiceNumber: int64
        [<ExactMatch("branch")>] Branch: string
        [<Date("date_published", "yyyy-MM-dd'T'HH:mm:ss.SSSZ")>] DatePublished: DateTime
        [<FullText("message_body")>] MessageBody: string
        [<FullText("request_message_id")>] RequestMessageId: string
    }

[<TestClass>]
type ElasticDslTests() =
    [<TestMethod>]
    member __.``Test Dsl.first`` () =
        let settings = {Node = Uri "http://localhost:9200"; DisableDirectStreaming = true; RequestTimeout = TimeSpan.FromMinutes 1.0; DefaultIndex = Some "sales_orders"; IndexMappings = [{IndexName = "sales_orders"; TypeName = "sales_order"; Type = typeof<SalesOrder>}]}
        let client = Elasticsearch.connect settings 
        let result = (MatchAll None) |> Dsl.first<SalesOrder> client None (FieldDirection ("id",Descending)) |> Operation.returnOrFail
        match result with
        | Some value -> Assert.IsTrue(value.Id > 0L)
        | None -> Assert.Fail("No SalesOrder returned by Dsl.first")