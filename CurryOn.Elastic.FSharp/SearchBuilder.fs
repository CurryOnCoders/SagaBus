namespace CurryOn.Elastic

open FSharp.Control

type Search<'index> = 
| BuildingSearch of SearchRequest
| Searching of Operation<'index seq, ElasticsearchEvent>
| CompletedSearch of OperationResult<'index seq, ElasticsearchEvent>



