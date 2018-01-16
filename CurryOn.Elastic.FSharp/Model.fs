namespace CurryOn.Elastic

open FSharp.Control
open System
open System.Linq

type TypeIndexMapping =
    {
        Type: Type
        IndexName: string
        TypeName: string
    } member this.IsMappingFor<'t> () = 
        this.Type.IsAssignableFrom(typeof<'t>)

type ElasticSettings =
    {
        Node: Uri
        DisableDirectStreaming: bool
        RequestTimeout: TimeSpan
        IndexMappings: TypeIndexMapping list
    } member internal this.GetConnectionSettings () =
        let connectionSettings = new Nest.ConnectionSettings(this.Node)
        let apply (settings: Nest.ConnectionSettings) =
            settings.RequestTimeout(this.RequestTimeout)
                    .MapDefaultTypeIndices(fun dict ->
                        this.IndexMappings |> List.fold (fun (acc: Nest.FluentDictionary<Type,string>) map -> acc.Add(map.Type, map.IndexName)) dict |> ignore)
        if this.DisableDirectStreaming
        then connectionSettings.DisableDirectStreaming() |> apply
        else connectionSettings |> apply                    

type CreateIndexRequest<'index when 'index: not struct> =
    {
        NumberOfShards: int option
        NumberOfReplicas: int option
        Settings: string*string list
    }

type DocumentId =
    | UniqueId of Guid
    | IntegerId of int64
    | StringId of string
    member internal this.ToId () =
        match this with
        | UniqueId guid -> guid |> Nest.Id.op_Implicit
        | IntegerId i -> i |> Nest.Id.op_Implicit
        | StringId s -> s |> Nest.Id.op_Implicit
    static member internal Parse (str: string) =
        match Guid.TryParse(str) with
        | (true, guid) -> UniqueId guid
        | _ -> match Int64.TryParse(str) with
               | (true, i) -> IntegerId i
               | _ -> StringId str
    static member internal From any =
        match any |> box with
        | :? Guid as guid -> UniqueId guid
        | :? int32 as i -> i |> int64 |> IntegerId
        | :? int64 as i -> IntegerId i
        | :? String as s -> DocumentId.Parse s
        | _ -> any.ToString() |> DocumentId.Parse

type SearchIndexScope =
| AllIndices
| Index of string
| Indices of string list

type SearchTypeScope =
| AllTypes
| Type of string
| Types of string list

type RangeTerm<'term> =
| Inclusive of 'term
| Exclusive of 'term
| Unbounded

type Range<'term when 'term :> IComparable> =
    {
        Minimum: RangeTerm<'term>
        Maximum: RangeTerm<'term>
    }

type FieldTerm =
| Exists
| Exactly of string
| Contains of string
| Proximity of string*int
| DateRange of Range<DateTime>
| IntegerRange of Range<int64>
| DecimalRange of Range<decimal>
| TagRange of Range<string>
| FieldAnd of FieldTerm*FieldTerm
| FieldOr of FieldTerm*FieldTerm
| FieldNot of FieldTerm
| FieldGroup of FieldTerm list

type QueryField =
    {
        Field: string
        Term: FieldTerm
    }

type QueryStringTerm =
| Empty
| Value of string
| FieldValue of QueryField
| QueryAnd of QueryStringTerm*QueryStringTerm
| QueryOr of QueryStringTerm*QueryStringTerm
| QueryNot of QueryStringTerm
| QueryGroup of QueryStringTerm list

type Operator =
    | And
    | Or
    member this.ToApi() =
        match this with
        | And -> Nest.Operator.And |> Nullable
        | Or -> Nest.Operator.Or |> Nullable

type SortDirection =
| Ascending
| Descending

type SearchSort =
| Field of string
| FieldDirection of string*SortDirection
| Score
| Document
| Multiple of SearchSort list

type QueryString =
    {
        Term: QueryStringTerm
        DefaultField: string option
        DefaultOperator: Operator option
    }

type FilterClause =
| FilterTerm of QueryField

type ZeroTermsQuery =
| ZeroTermsNone
| ZeroTermsAll

type MatchClause =
    {
        Field: string
        Query: string
        Operator: Operator option
        ZeroTerms: ZeroTermsQuery option
    }

type MatchPhrase =
    {
        Field: string
        Phrase: string
    }

type MatchPhrasePrefix =
    {
        Field: string
        PhrasePrefix: string
        MaxExpansions: int option
    }

type MultiMatchType =
| BestFields
| CrossFields
| MostFields
| Phrase
| PhrasePrefix

type MultiMatchQuery =
    {
        Query: string
        Type: MultiMatchType
        Fields: string list
        Operator: Operator option
        MinmumShouldMatch: float option
    }

type CommonTermsQuery =
    {
        Field: string
        Query: string
        CutoffFrequency: float option
        MinimumShouldMatch: float option
        LowFrequencyOperator: Operator option
    }

type Flags =
| AllFlag
| NoneFlag
| AndFlag
| OrFlag
| NotFlag
| PrefixFlag
| PhraseFlag
| PrecedenceFlag
| EscapeFlag
| WhitespaceFlag
| FuzzyFlag
| NearFlag
| SlopFlag

type SimpleQueryStringQuery =
    {
        Fields: string list
        Query: string
        Flags: Flags list
        AutoGenerateSynonyms: bool
    }

type TermQuery =
    {
        Field: string
        Value: obj
        Boost: float option
    }

type TermsQuery =
    {
        Field: string
        Values: obj list
    }

type TermsSetQuery =
    {
        Field: string
        Terms: obj list
        MinimumShouldMatchField: string option
    }

type LowerRange<'term when 'term :> IComparable> =
| UnboundedLower
| GreaterThan of 'term
| GreaterThanOrEqual of 'term

type UpperRange<'term when 'term :> IComparable> =
| UnboundedUpper
| LessThan of 'term
| LessThanOrEqual of 'term

type RangeQuery<'term when 'term :> IComparable> =
    {
        LowerRange: LowerRange<'term>
        UpperRange: UpperRange<'term>
        Boost: float option
        Format: string option
        TimeZone: string option
    }

type PrefixQuery = 
    {
        Field: string
        Prefix: string
        Boost: float option
    }

type WildcardQuery =
    {
        Field: string
        Pattern: string
        Boost: float option
    }

type RegexFlags =
| RegexAll
| AnyString
| Complement
| RegexEmpty
| Intersection
| Interval
| RegexNone

type RegexQuery =
    {
        Field: string
        Regex: string
        Flags: RegexFlags list
        Boost: float option
        MaxDeterminizedStates: int option
    }

type FuzzyQuery =
    {
        Field: string
        Value: string
        Boost: float option
        Fuzziness: int option
        PrefixLength: int option
        MaxExpansions: int option
    }

type IdsQuery =
    {
        Type: string
        Values: DocumentId list
    }

type BoostMode =
    | Multiply
    | Replace
    | Sum
    | Average
    | MaxBoost
    | MinBoost
    member internal this.ToApi() =
        match this with
        | Multiply -> Nest.FunctionBoostMode.Multiply
        | Replace -> Nest.FunctionBoostMode.Replace
        | Sum -> Nest.FunctionBoostMode.Sum
        | Average -> Nest.FunctionBoostMode.Average
        | MaxBoost -> Nest.FunctionBoostMode.Max
        | MinBoost -> Nest.FunctionBoostMode.Min

type ScoreMode =
    | MultiplyScore
    | SumScore
    | AverageScore
    | FirstScore
    | MaxScore
    | MinScore
    member internal this.ToApi() =
        match this with
        | MultiplyScore -> Nest.ScoreMode.Multiply
        | SumScore -> Nest.ScoreMode.Total
        | AverageScore -> Nest.ScoreMode.Average
        | FirstScore -> Nest.ScoreMode.Total
        | MaxScore -> Nest.ScoreMode.Max
        | MinScore -> Nest.ScoreMode.Min
    member internal this.ToNested() =
        match this with
        | MultiplyScore -> Nest.NestedScoreMode.None
        | SumScore -> Nest.NestedScoreMode.Sum
        | AverageScore -> Nest.NestedScoreMode.Average
        | FirstScore -> Nest.NestedScoreMode.None
        | MaxScore -> Nest.NestedScoreMode.Max
        | MinScore -> Nest.NestedScoreMode.Min
    member internal this.ToFunctionScoreMode() =
        match this with
        | MultiplyScore -> Nest.FunctionScoreMode.Multiply
        | SumScore -> Nest.FunctionScoreMode.Sum
        | AverageScore -> Nest.FunctionScoreMode.Average
        | FirstScore -> Nest.FunctionScoreMode.First
        | MaxScore -> Nest.FunctionScoreMode.Max
        | MinScore -> Nest.FunctionScoreMode.Min

type BoolQuery =
    {
        Must: QueryClause list
        Filter: QueryClause list
        Should: QueryClause list
        MustNot: QueryClause list
    }

and DisMaxQuery =
    {
        Queries: QueryClause list
        TieBreaker: float option
        Boost: float option
    }

and FunctionScoreQuery =
    {
        Query: QueryClause
        Boost: float option
        MaxBoost: float option
        BoostMode: BoostMode
        ScoreMode: ScoreMode
        MinScore: float option
    }

and BoostingQuery =
    {
        Positive: QueryClause
        Negative: QueryClause
        NegativeBoost: float option
    }

and NestedQuery =
    {
        Path: string
        ScoreMode: ScoreMode
        Query: QueryClause
    }

and HasChildQuery =
    {
        Type: string
        Query: QueryClause
    }

and HasParentQuery =
    {
        ParentType: string
        Query: QueryClause
    }

and ParentIdQuery =
    {
        Type: string
        Id: DocumentId
    }

and QueryClause =
| MatchAll of float option
| MatchNone
| Match of MatchClause
| MatchPhrase of MatchPhrase
| MatchPhrasePrefix of MatchPhrasePrefix
| MultiMatch of MultiMatchQuery
| CommonTerms of CommonTermsQuery
| QueryString of QueryString
| SimpleQueryString of SimpleQueryStringQuery
| Term of TermQuery
| Terms of TermsQuery
| TermsSet of TermsSetQuery
| DateRangeQuery of RangeQuery<DateTime>
| NumericRangeQuery of RangeQuery<float>
| StringRangeQuery of RangeQuery<string>
| HasValue of string
| Prefix of PrefixQuery
| Wildcard of WildcardQuery
| Regexp of RegexQuery
| Fuzzy of FuzzyQuery
| TypeQuery of string
| Ids of IdsQuery
| ConstantScore of QueryClause*int
| Bool of BoolQuery
| DisMax of DisMaxQuery
| FunctionScore of FunctionScoreQuery
| Boosting of BoostingQuery
| Nested of NestedQuery
| HasChild of HasChildQuery
| HasParent of HasParentQuery
| ParentId of ParentIdQuery

type SearchStructure =
| QueryStringQuery of QueryString
| RequestBodyQuery of QueryClause

type SearchRequest =
    {
        Search: SearchStructure
        Timeout: TimeSpan option
        Sort: SearchSort option
        From: int option
        Size: int option
    }    

type ShardInfo =
    {
        Total: int
        Successful: int
        Failed: int
    }

type Hit<'index when 'index: not struct> =
    {
        Index: string
        Type: string
        Id: DocumentId
        Score: float option
        Document: 'index
    }

type ResultHits<'index when 'index: not struct> =
    {
        TotalHits: int64
        MaximumScore: float option
        Hits: Hit<'index> list
    }

type SearchResult<'index when 'index: not struct> =
    {
        ElapsedTime: TimeSpan
        TimedOut: bool
        Shards: ShardInfo 
        Results: ResultHits<'index>
        ScrollId: string
    }

type ElasticsearchServerError =
    {
        Type: string
        Resource: string option
        Reason: string
        Cause: ElasticsearchServerError option
    }

type ElasticError =
    {
        Status: int
        OriginalException: exn option
        Error: ElasticsearchServerError
    }

type ElasticIndexResult =
    | Created
    | Updated
    | Deleted
    | NotFound
    | NoOp
    | IndexError of ElasticsearchServerError
    static member internal FromApi (result: Nest.Result) =
        match result with
        | Nest.Result.Created -> Created
        | Nest.Result.Deleted -> Deleted
        | Nest.Result.Updated -> Updated
        | Nest.Result.NotFound -> NotFound
        | _ -> NoOp

type GetRequest<'index when 'index: not struct> =
    {
        Id: DocumentId
        StoredFields: string list
        FetchDocument: bool
    }

type GetResponse<'index when 'index: not struct> =
    {
        Index: string
        Type: string
        Id: DocumentId
        Version: int64
        Found: bool
        Document: 'index option
        Fields: (string*obj) list
    }

type DeleteRequest<'index when 'index: not struct> =
    {
        Id: DocumentId
        Timeout: TimeSpan option
        Version: int64 option
    }

type DeleteResponse =
    {
        Index: string
        Type: string
        Id: DocumentId
        Version: int64
        Result: ElasticIndexResult
    }

type UpdateScript =
    {
        Source: string
        Lang: string
        Params: string*string list
    }

type Upsert<'index when 'index: not struct> =
    {
        Document: 'index
        Script: UpdateScript option
    }

type Update<'index when 'index: not struct> =
| UpdateScript of UpdateScript
| PartialDocument of 'index
| Upsert of Upsert<'index>
| DocumentAsUpsert of 'index

type UpdateRequest<'index when 'index: not struct> =
    {
        Id: DocumentId
        Update: Update<'index>
        RetryOnConflit: bool
        Timeout: TimeSpan option
        IncludeUpdatedDocumentInResponse: bool
        Version: int64 option
        Force: bool
    }

type UpdateResponse<'index when 'index: not struct> =
    {
        Shards: ShardInfo
        Index: string
        Type: string
        Id: DocumentId
        Version: int64
        Result: ElasticIndexResult
        Document: 'index option
    }

type IndexRequest<'index when 'index: not struct> =
    {
        Document: 'index
        Id: DocumentId option
    }

type IndexResponse =
    {
        Index: string
        Type: string
        Id: DocumentId
        Version: int64
        Result: ElasticIndexResult
    }

type BulkIndexResponse =
    {
        ElapsedTime: TimeSpan
        Errors: bool
        Results: IndexResponse list
    }
    
type ElasticsearchEvent =
| IndexCreated
| IndexAlreadyExists
| IndexCreationFailed of ElasticError
| IndexDeleted
| IndexDoesNotExist
| IndexExistsFailed of ElasticError
| IndexDeletionFailed of ElasticError
| DocumentIndexed
| DocumentAlreadyIndexed
| IndexingFailed of ElasticError
| IdConflit of DocumentId
| BulkDocumentsIndexed
| BulkIndexingFailed of ElasticError
| OldDocumentsDeleted
| DeletingOldDocumentsFailed of ElasticError
| DocumentUpdated
| VersionConflict of int64*int64
| UpdateFailed of ElasticError
| DocumentRetrieved
| DocumentNotFound
| GetDocumentFailed of ElasticError
| DocumentDeleted
| DeleteFailed of ElasticError
| SearchExecutedSuccessfully
| QueryParseError of ElasticError
| QueryExecutionError of ElasticError
| UnhandledException of exn

type IElasticClient =
    inherit IDisposable
    /// Checks to see if an index for the given type already exists in Elasticsearch
    abstract member IndexExists<'index when 'index: not struct> : unit -> Operation<bool, ElasticsearchEvent>
    /// Creates an index for the given type in the Elasticsearch repository
    abstract member CreateIndex<'index when 'index: not struct> : unit -> Operation<unit, ElasticsearchEvent>
    /// Creates an index for the given type with the specified parameters in the Elasticsearch repository
    abstract member CreateIndex<'index when 'index: not struct> : CreateIndexRequest<'index> -> Operation<unit, ElasticsearchEvent>
    /// Deletes the the index for the given type and all documents contained therein from Elasticsearch
    abstract member DeleteIndex<'index when 'index: not struct> : unit -> Operation<unit, ElasticsearchEvent>
    /// Deletes and Recreates the index for the given type in Elasticsearch
    abstract member RecreateIndex<'index when 'index: not struct> : unit -> Operation<unit, ElasticsearchEvent>
    /// Deletes all documents in the index for the given type older than the specified date
    abstract member DeleteOldDocuments<'index when 'index: not struct> : DateTime -> Operation<unit, ElasticsearchEvent>
    /// Indexes the given document, adding it to Elasticsearch
    abstract member Index<'index when 'index: not struct> : IndexRequest<'index> -> Operation<IndexResponse, ElasticsearchEvent>
    /// Bulk-Indexes all given documents, adding them to Elasticsearch using the specified IDs and metadata
    abstract member BulkIndex<'index when 'index: not struct and 'index: equality> : IndexRequest<'index> seq -> Operation<BulkIndexResponse, ElasticsearchEvent>
    /// Bulk-Indxes all given documents, adding them to Elasticsearch with dynamically generated IDs and metadata
    abstract member BulkIndex<'index when 'index: not struct> : 'index seq -> Operation<BulkIndexResponse, ElasticsearchEvent>
    /// Retrieves the document with the specified ID from the Elasticsearch index
    abstract member Get<'index when 'index: not struct> : GetRequest<'index> -> Operation<GetResponse<'index>, ElasticsearchEvent>
    /// Deletes the specified document from the Elasticsearch index
    abstract member Delete<'index when 'index: not struct> : DeleteRequest<'index> -> Operation<DeleteResponse, ElasticsearchEvent>
    /// Updates the specified document in the Elasticsearch index, performing either a scripted update, field-level updates, or an Upsert
    abstract member Update<'index when 'index: not struct> : UpdateRequest<'index> -> Operation<UpdateResponse<'index>, ElasticsearchEvent>
    /// Executes a search against the specified Elasticsearch index
    abstract member Search<'index when 'index: not struct> : SearchRequest -> Operation<SearchResult<'index>, ElasticsearchEvent>
