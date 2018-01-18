namespace CurryOn.Elastic

open Elasticsearch.Net
open FSharp.Control
open Nest
open System

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Elastic =
    let private toIndices : IndexName -> Indices = Indices.op_Implicit
    let private promiseConverter<'a, 'b when 'a: not struct and 'b: not struct> (promise: IPromise<'a>) = {new IPromise<'b> with member __.Value = promise.Value |> unbox<'b>}
    let indexName<'index when 'index: not struct> = IndexName.From<'index>()
    let indices<'index when 'index: not struct> = toIndices <| indexName<'index>
    let field: string -> Field = Field.op_Implicit
    let inline fields names = names |> Seq.map field |> Seq.toArray |> Fields.op_Implicit
    let date: DateTime -> DateMath = DateMath.op_Implicit 
    let time: TimeSpan -> Time = Time.op_Implicit
    let minimumShouldMatch: float -> MinimumShouldMatch = MinimumShouldMatch.op_Implicit
    let typeName: string -> TypeName = TypeName.op_Implicit

    let rec private getCause (cause: Elasticsearch.Net.CausedBy) =
        { Type = cause.Type
          Resource = None
          Reason = cause.Reason
          Cause = if cause.InnerCausedBy |> isNotNull then cause.InnerCausedBy |> getCause |> Some else None
        }

    let rec private getBulkCause (cause: Nest.CausedBy) =
        { Type = cause.Type
          Resource = None
          Reason = cause.Reason
          Cause = if cause.InnerCausedBy |> isNotNull then cause.InnerCausedBy |> getBulkCause |> Some else None
        }

    let inline private toServerError (error: Error) =
        { Type = error.Type
          Resource = Some error.ResourceType
          Reason = error.Reason
          Cause = if error.CausedBy |> isNotNull then error.CausedBy |> getCause |> Some else None
        }

    let inline private toBulkError (error: BulkError) =
        { Type = error.Type
          Resource = None
          Reason = error.Reason
          Cause = if error.CausedBy |> isNotNull then error.CausedBy |> getBulkCause |> Some else None
        }

    let inline private toError (response: IResponse) =
        { Status = response.ServerError.Status
          OriginalException = if response.OriginalException |> isNotNull then Some response.OriginalException else None
          Error = response.ServerError.Error |> toServerError
        }

    let inline private toFlags (flags: Flags seq) =
        let inline getFlag (flag: Flags) =
            match flag with
            | AllFlag -> SimpleQueryStringFlags.All
            | NoneFlag -> SimpleQueryStringFlags.None
            | AndFlag -> SimpleQueryStringFlags.And
            | OrFlag -> SimpleQueryStringFlags.Or
            | NotFlag -> SimpleQueryStringFlags.Not
            | PrefixFlag -> SimpleQueryStringFlags.Prefix
            | PhraseFlag -> SimpleQueryStringFlags.Phrase
            | PrecedenceFlag -> SimpleQueryStringFlags.Precedence
            | EscapeFlag -> SimpleQueryStringFlags.Escape
            | WhitespaceFlag -> SimpleQueryStringFlags.Whitespace
            | FuzzyFlag -> SimpleQueryStringFlags.Fuzzy
            | NearFlag -> SimpleQueryStringFlags.Near
            | SlopFlag -> SimpleQueryStringFlags.Slop
            |> int
        flags |> Seq.fold (fun acc cur -> ((acc |> int) &&& (getFlag cur)) |> enum<SimpleQueryStringFlags>) (0 |> enum<SimpleQueryStringFlags>)

    let internal indexExists<'index when 'index: not struct> (client: ElasticClient) : Operation<bool,ElasticsearchEvent> =
        operation {
            let index = indices<'index>
            let! response = client.IndexExistsAsync(index)
            return! if response.IsValid
                    then Result.success response.Exists
                    else Result.failure [response |> toError |> IndexExistsFailed]
        }

    let internal toCreateIndexRequest (settings: CreateIndexRequest<_>) (cd: CreateIndexDescriptor) =
        cd.Settings(fun s -> s.NumberOfReplicas(settings.NumberOfReplicas |> toNullable).NumberOfShards(settings.NumberOfShards |> toNullable) |> promiseConverter<IndexSettings, IIndexSettings>) :> ICreateIndexRequest

    let internal createIndex<'index when 'index: not struct> (client: ElasticClient) (details: CreateIndexRequest<'index> option) =
        operation {
            let index = indexName<'index>
            let! indexAlreadyExists = indexExists<'index> client
            if indexAlreadyExists
            then return! Result.failure [IndexAlreadyExists]
            else let! response =
                     match details with
                     | Some settings -> client.CreateIndexAsync(index, toCreateIndexRequest settings)
                     | None -> client.CreateIndexAsync(index)
                 return! if response.IsValid
                         then Result.successWithEvents () [IndexCreated]
                         else Result.failure [response |> toError |> IndexCreationFailed]
        }        

    let internal deleteIndex<'index when 'index: not struct> (client: ElasticClient) =
        operation {
            let index = indices<'index>
            let! indexExists = indexExists<'index> client
            if indexExists 
            then let! response = client.DeleteIndexAsync(index)
                 return! if response.IsValid
                         then Result.successWithEvents () [IndexDeleted]
                         else Result.failure [response |> toError |> IndexDeletionFailed]
            else return! Result.failure [IndexDoesNotExist]
        }

    let internal recreateIndex<'index when 'index: not struct> (client: ElasticClient) =
        operation {
            let! indexExists = indexExists<'index> client
            if indexExists
            then do! deleteIndex<'index> client
                 return! createIndex<'index> client None
            else return! Result.failure [IndexDoesNotExist]
        }

    let internal deleteOldDocuments<'index when 'index: not struct> (client: ElasticClient) (date: DateTime) =
        operation {
            let! response = client.DeleteByQueryAsync<'index>(fun q -> q.Query(fun qs -> qs.DateRange(fun r -> r.LessThan(DateMath.Anchored(date)) :> IDateRangeQuery)) :> IDeleteByQueryRequest);
            return! if response.IsValid
                    then Result.successWithEvents () [OldDocumentsDeleted]
                    else Result.failure [response |> toError |> DeletingOldDocumentsFailed]
        }

    let internal index<'index when 'index: not struct> (client: ElasticClient) (indexRequest: CurryOn.Elastic.IndexRequest<'index>) =
        operation {
            let! response = 
                match indexRequest.Id with
                | Some id -> client.IndexAsync<'index>(indexRequest.Document, (fun i -> i.Id(id.ToId()) :> IIndexRequest))
                | None -> client.IndexAsync<'index>(indexRequest.Document)
            return! if response.IsValid && response.Created
                    then let indexResponse =
                            { Index = response.Index
                              Type = response.Type
                              Id = response.Id |> DocumentId.Parse
                              Version = response.Version
                              Result = response.Result |> ElasticIndexResult.FromApi
                            }
                         Result.successWithEvents indexResponse [DocumentIndexed]
                    else Result.failure [response |> toError |> IndexingFailed]
        }

    let inline private parseBulkResponse (response: IBulkResponse) =
        { ElapsedTime = TimeSpan.FromMilliseconds(response.Took |> float)
          Errors = response.Errors
          Results = response.Items 
                  |> Seq.map (fun item -> {Index = item.Index; Type = item.Type; Id = item.Id |> DocumentId.Parse; Version = item.Version; Result = if item.IsValid then Created else IndexError (item.Error |> toBulkError)})
                  |> Seq.toList
        }

    let internal bulkIndex<'index when 'index: not struct and 'index: equality> (client: ElasticClient) (indexRequests: CurryOn.Elastic.IndexRequest<'index> seq) =
        operation {
            let getId (create: BulkCreateDescriptor<'index>) (document: 'index) =
                match indexRequests |> Seq.tryFind (fun request -> request.Document = document) with
                | Some request ->
                    match request.Id with
                    | Some id -> create.Id(id.ToId()) :> IBulkCreateOperation<'index>
                    | None -> create :> IBulkCreateOperation<'index>
                | None -> create :> IBulkCreateOperation<'index>
            let getBulkOperation (descriptor: BulkDescriptor) =
                descriptor.CreateMany(indexRequests |> Seq.map (fun request -> request.Document), fun bd index -> getId bd index) :> IBulkRequest
            let! response = client.BulkAsync(fun b -> getBulkOperation b)
            return! if response.IsValid 
                    then let bulkResponse = response |> parseBulkResponse                            
                         Result.successWithEvents bulkResponse [BulkDocumentsIndexed]
                    else Result.failure [response |> toError |> BulkIndexingFailed]
        }

    let internal bulkIndexDocuments<'index when 'index: not struct> (client: ElasticClient) (documents: 'index seq) =
        operation {
            let! response = client.BulkAsync(fun b -> b.CreateMany(documents) :> IBulkRequest)
            return! if response.IsValid 
                    then let bulkResponse = response |> parseBulkResponse
                         Result.successWithEvents bulkResponse [BulkDocumentsIndexed]
                    else Result.failure [response |> toError |> BulkIndexingFailed]
        }

    let internal get<'index when 'index: not struct> (client: ElasticClient) (request: CurryOn.Elastic.GetRequest<'index>) =
        operation {
            let documentPath = DocumentPath<'index>.Id(request.Id.ToId())
            let! response = 
                match request.StoredFields with
                | [] -> client.GetAsync<'index>(documentPath, fun gd -> gd.SourceEnabled(request.FetchDocument |> string) :> IGetRequest)
                | fields -> client.GetAsync<'index>(documentPath, fun gd -> gd.StoredFields(fields |> List.toArray) :> IGetRequest)
            return! if response.IsValid
                    then if response.Found
                         then let getResponse =
                                { Index = response.Index
                                  Type = response.Type
                                  Id = DocumentId.Parse response.Id
                                  Version = response.Version
                                  Found = response.Found
                                  Document = if request.FetchDocument then Some response.Source else None
                                  Fields = if request.StoredFields.IsEmpty then [] else response.Fields |> Seq.map (fun f -> f.Key,f.Value) |> Seq.toList
                                }
                              Result.successWithEvents getResponse [DocumentRetrieved]
                         else Result.failure [DocumentNotFound]
                    else Result.failure [response |> toError |> GetDocumentFailed]
                 
        }

    let internal delete<'index when 'index: not struct> (client: ElasticClient) (request: CurryOn.Elastic.DeleteRequest<'index>) =
        operation {
            let documentPath = DocumentPath<'index>.Id(request.Id.ToId())
            let! response = 
                match request.Timeout with
                | Some timeout ->
                    match request.Version with
                    | Some version -> client.DeleteAsync(documentPath, fun d -> d.Timeout(time timeout).Version(version) :> IDeleteRequest)
                    | None -> client.DeleteAsync(documentPath, fun d -> d.Timeout(time timeout) :> IDeleteRequest)
                | None ->
                    match request.Version with
                    | Some version -> client.DeleteAsync(documentPath, fun d -> d.Version(version) :> IDeleteRequest)
                    | None -> client.DeleteAsync(documentPath)
            return! if response.IsValid
                    then if response.Found
                         then let deleteResponse =
                                { DeleteResponse.Index = response.Index
                                  Type = response.Type
                                  Id = DocumentId.Parse response.Id
                                  Version = match Int64.TryParse response.Version with
                                            | (true, version) -> version
                                            | _ -> 0L
                                  Result = response.Result |> ElasticIndexResult.FromApi
                                }
                              Result.successWithEvents deleteResponse [DocumentDeleted]
                         else Result.failure [DocumentNotFound]
                    else Result.failure [response |> toError |> DeleteFailed]

        }

    let internal update<'index when 'index: not struct> (client: ElasticClient) (request: UpdateRequest<'index>) =
        operation {
            let documentPath = DocumentPath<'index>.Id(request.Id.ToId())
            let retryOnConflict = if request.RetryOnConflit then 5L else 0L
            let getUpdate (ud: UpdateDescriptor<'index,'index>) =
                match request.Timeout with
                | Some timeout ->
                    match request.Version with
                    | Some version -> ud.Timeout(time timeout).Version(version).RetryOnConflict(retryOnConflict) :> IUpdateRequest<'index,'index>
                    | None -> ud.Timeout(time timeout).RetryOnConflict(retryOnConflict) :> IUpdateRequest<'index,'index>
                | None -> 
                    match request.Version with
                    | Some version -> ud.Version(version).RetryOnConflict(retryOnConflict) :> IUpdateRequest<'index,'index>
                    | None -> ud.RetryOnConflict(retryOnConflict) :> IUpdateRequest<'index,'index>

            let inline partialDocument document (ud: UpdateDescriptor<'index,'index>) = ud.Doc(document)            
            let inline upsert document (ud: UpdateDescriptor<'index,'index>) = ud.Upsert(document)
            let inline docAsUpsert document (ud: UpdateDescriptor<'index,'index>) = ud.DocAsUpsert(true) |> partialDocument document

            let inline updateScript (script: UpdateScript) (ud: UpdateDescriptor<'index,'index>)  =
                ud.Lang(script.Lang).Script(fun s -> s.Inline(script.Source) :> IScript)

            let inline scriptedUpsert script document (ud: UpdateDescriptor<'index,'index>) =
                ud.ScriptedUpsert(true) |> upsert document |> updateScript script            

            let! response = 
                match request.Update with
                | UpdateScript script -> client.UpdateAsync<'index>(documentPath, fun ud -> ud |> updateScript script |> getUpdate)
                | PartialDocument partial -> client.UpdateAsync<'index>(documentPath, fun ud -> ud |> partialDocument partial |> getUpdate)
                | Upsert update -> 
                    match update.Script with
                    | Some script -> client.UpdateAsync<'index>(documentPath, fun ud -> ud |> scriptedUpsert script update.Document |> getUpdate)
                    | None -> client.UpdateAsync<'index>(documentPath, fun ud -> ud |> upsert update.Document |> getUpdate)
                | DocumentAsUpsert upsertDoc -> client.UpdateAsync<'index>(documentPath, fun ud -> ud |> docAsUpsert upsertDoc |> getUpdate)
                    
            return! if response.IsValid
                    then let updateResponse =
                            { Shards = {Total = response.ShardsHit.Total; Successful = response.ShardsHit.Successful; Failed = response.ShardsHit.Failed}
                              Index = response.Index
                              Type = response.Type
                              Id = response.Id |> DocumentId.Parse
                              Version = response.Version
                              Result = response.Result |> ElasticIndexResult.FromApi
                              Document = if response.Get.Found
                                         then Some response.Get.Source
                                         else None
                            }
                         Result.successWithEvents updateResponse [DocumentUpdated]
                    else Result.failure [response |> toError |> UpdateFailed]
        }

    let inline applyQueryStringToDescriptor<'index when 'index: not struct> (queryString: QueryString) (query: QueryStringQueryDescriptor<'index>) =
        let queryFieldToString (queryField: QueryField) =
            let rec fieldTermToString (term: FieldTerm) =
                match term with
                | Exists -> sprintf "_exists_:%s" queryField.Field 
                | Exactly value -> sprintf "\"%s\"" value
                | Contains value -> sprintf "%s" value
                | Proximity (value,distance) -> sprintf "\"%s\"~%d" value distance
                | DateRange dateRange -> 
                    match dateRange.Minimum with
                    | Inclusive min ->
                        match dateRange.Maximum with
                        | Inclusive max -> sprintf "[%A TO %A]" min max
                        | Exclusive max -> sprintf "[%A TO %A}" min max
                        | Unbounded ->sprintf "[%A to *]" min
                    | Exclusive min ->
                        match dateRange.Maximum with
                        | Inclusive max -> sprintf "{%A TO %A]" min max
                        | Exclusive max -> sprintf "{%A TO %A}" min max
                        | Unbounded -> sprintf "{%A to *}" min
                    | Unbounded ->
                        match dateRange.Maximum with
                        | Inclusive max -> sprintf "[* TO %A]" max
                        | Exclusive max -> sprintf "[* TO %A}" max 
                        | Unbounded -> "[* to *]"
                | IntegerRange intRange ->
                    match intRange.Minimum with
                    | Inclusive min ->
                        match intRange.Maximum with
                        | Inclusive max -> sprintf "[%d TO %d]" min max
                        | Exclusive max -> sprintf "[%d TO %d}" min max
                        | Unbounded -> sprintf "[%d to *]" min
                    | Exclusive min ->
                        match intRange.Maximum with
                        | Inclusive max -> sprintf "{%d TO %d]" min max
                        | Exclusive max -> sprintf "{%d TO %d}" min max
                        | Unbounded -> sprintf "{%d to *}" min
                    | Unbounded ->
                        match intRange.Maximum with
                        | Inclusive max -> sprintf "[* TO %d]" max
                        | Exclusive max -> sprintf "[* TO %d}" max
                        | Unbounded -> "[* to *]"
                | DecimalRange decRange ->
                    match decRange.Minimum with
                    | Inclusive min ->
                        match decRange.Maximum with
                        | Inclusive max -> sprintf "[%f TO %f]" min max
                        | Exclusive max -> sprintf "[%f TO %f}" min max
                        | Unbounded -> sprintf "[%f to *]" min
                    | Exclusive min ->
                        match decRange.Maximum with
                        | Inclusive max -> sprintf "{%f TO %f]" min max
                        | Exclusive max -> sprintf "{%f TO %f}" min max
                        | Unbounded -> sprintf "{%f to *}" min
                    | Unbounded ->
                        match decRange.Maximum with
                        | Inclusive max -> sprintf "[* TO %f]" max
                        | Exclusive max -> sprintf "[* TO %f}" max
                        | Unbounded -> "[* to *]"
                | TagRange tagRange ->
                    match tagRange.Minimum with
                    | Inclusive min ->
                        match tagRange.Maximum with
                        | Inclusive max -> sprintf "[%A TO %A]" min max
                        | Exclusive max -> sprintf "[%A TO %A}" min max
                        | Unbounded -> sprintf "[%A to *]" min
                    | Exclusive min ->
                        match tagRange.Maximum with
                        | Inclusive max -> sprintf "{%A TO %A]" min max
                        | Exclusive max -> sprintf "{%A TO %A}" min max
                        | Unbounded -> sprintf "{%A to *}" min
                    | Unbounded ->
                        match tagRange.Maximum with
                        | Inclusive max -> sprintf "[* TO %A]" max
                        | Exclusive max -> sprintf "[* TO %A}" max
                        | Unbounded -> "[* to *]"
                | FieldAnd (left,right) -> sprintf "(%s) AND (%s)" (fieldTermToString left) (fieldTermToString right)
                | FieldOr (left,right) -> sprintf "(%s) OR (%s)" (fieldTermToString left) (fieldTermToString right)
                | FieldNot notTerm -> sprintf "NOT (%s)" (fieldTermToString notTerm)
                | FieldGroup terms -> 
                    let inline reduceTerms operator =
                        match terms with
                        | [] -> String.Empty
                        | head::tail -> tail |> Seq.fold (fun acc cur -> sprintf "%s %s (%s)" acc operator <| fieldTermToString cur) (fieldTermToString head)
                    match queryString.DefaultOperator with
                    | Some operator ->
                        match operator with
                        | And -> reduceTerms "AND"
                        | Or -> reduceTerms "OR"
                    | None -> reduceTerms "OR"
            match queryField.Term with
            | Exists -> fieldTermToString queryField.Term
            | _ -> sprintf "%s:%s" queryField.Field <| fieldTermToString queryField.Term
        let inline applyQueryStringTerms (term: QueryStringTerm) (query: QueryStringQueryDescriptor<'index>) =
            let rec queryTermToString (term: QueryStringTerm) =
                match term with
                | Empty -> String.Empty 
                | QueryStringTerm.Value value -> value
                | FieldValue fieldValue -> queryFieldToString fieldValue
                | QueryAnd (left,right) -> sprintf "(%s) AND (%s)" (queryTermToString left) (queryTermToString right)
                | QueryOr (left,right) -> sprintf "(%s) OR (%s)" (queryTermToString left) (queryTermToString right)
                | QueryNot notTerm -> sprintf "NOT (%s)" <| queryTermToString notTerm
                | QueryGroup terms -> 
                    let inline reduceTerms operator =
                        match terms with
                        | [] -> String.Empty
                        | head::tail -> tail |> Seq.fold (fun acc cur -> sprintf "%s %s (%s)" acc operator <| queryTermToString cur) (queryTermToString head) 
                    match queryString.DefaultOperator with
                    | Some operator ->
                        match operator with
                        | And -> reduceTerms "AND"
                        | Or -> reduceTerms "OR"
                    | None -> reduceTerms "OR"
            match term with
            | Empty -> query 
            | QueryStringTerm.Value value -> query.AllFields().Query(value)
            | _ -> query.Query(queryTermToString term)
        let filteredQuery = query |> applyQueryStringTerms queryString.Term
        match queryString.DefaultField with
        | Some defaultField ->
            match queryString.DefaultOperator with
            | Some defaultOperator -> filteredQuery.DefaultField(field defaultField).DefaultOperator(defaultOperator.ToApi()) :> IQueryStringQuery
            | None -> filteredQuery.DefaultField(field defaultField) :> IQueryStringQuery
        | None ->
            match queryString.DefaultOperator with
            | Some defaultOperator -> filteredQuery.DefaultOperator(defaultOperator.ToApi()) :> IQueryStringQuery
            | None -> filteredQuery :> IQueryStringQuery
                    
    let rec applyQueryClauseToDescriptor<'index when 'index: not struct> (queryClause: QueryClause) (search: QueryContainerDescriptor<'index>) =
        let applyMatchClause (matchClause: MatchClause) (query: QueryContainerDescriptor<'index>) =
            query.Match(fun m -> 
                let matched = m.Field(field matchClause.Field).Query(Convert.ToString(matchClause.Query))
                match matchClause.Operator with
                | Some operator ->
                    match operator with
                    | And -> 
                        match matchClause.ZeroTerms with
                        | Some zeroTerms ->
                            match zeroTerms with
                            | ZeroTermsAll -> matched.Operator(Operator.And |> Nullable).ZeroTermsQuery(ZeroTermsQuery.All |> Nullable)
                            | ZeroTermsNone -> matched.Operator(Operator.And |> Nullable).ZeroTermsQuery(ZeroTermsQuery.None |> Nullable)
                        | None -> matched.Operator(Operator.And |> Nullable)
                    | Or -> 
                        match matchClause.ZeroTerms with
                        | Some zeroTerms ->
                            match zeroTerms with
                            | ZeroTermsAll -> matched.Operator(Operator.Or |> Nullable).ZeroTermsQuery(ZeroTermsQuery.All |> Nullable)
                            | ZeroTermsNone -> matched.Operator(Operator.Or |> Nullable).ZeroTermsQuery(ZeroTermsQuery.None |> Nullable)
                        | None -> matched.Operator(Operator.Or |> Nullable)
                | None ->
                    match matchClause.ZeroTerms with
                    | Some zeroTerms -> 
                        match zeroTerms with
                        | ZeroTermsAll -> matched.ZeroTermsQuery(ZeroTermsQuery.All |> Nullable)
                        | ZeroTermsNone -> matched.ZeroTermsQuery(ZeroTermsQuery.None |> Nullable)
                    | None -> matched
                :> IMatchQuery)
        match queryClause with
        | MatchAll boost -> search.MatchAll(fun m -> m.Boost(boost |> toNullable) :> IMatchAllQuery)
        | MatchNone -> search.MatchNone()
        | Match matchClause -> search |> applyMatchClause matchClause
        | MatchPhrase matchPhrase -> search.MatchPhrase(fun mp -> mp.Field(field matchPhrase.Field).Query(matchPhrase.Phrase) :> IMatchQuery)
        | MatchPhrasePrefix prefix -> search.MatchPhrasePrefix(fun mp -> mp.Field(field prefix.Field).Query(prefix.PhrasePrefix).MaxExpansions(prefix.MaxExpansions |> toNullable) :> IMatchQuery)
        | MultiMatch multiMatch -> search.MultiMatch(fun m -> 
            let start = m.Fields(fields multiMatch.Fields).Query(multiMatch.Query)
            let matches = 
                match multiMatch.MinmumShouldMatch with
                | Some min -> start.MinimumShouldMatch(minimumShouldMatch min)
                | None -> start
            match multiMatch.Operator with
            | Some operator -> 
                match operator with
                | And -> 
                    match multiMatch.Type with
                    | BestFields -> matches.Type(TextQueryType.BestFields |> Nullable).Operator(Nest.Operator.And |> Nullable)
                    | CrossFields -> matches.Type(TextQueryType.CrossFields |> Nullable).Operator(Nest.Operator.And |> Nullable)
                    | MostFields -> matches.Type(TextQueryType.MostFields |> Nullable).Operator(Nest.Operator.And |> Nullable)
                    | Phrase -> matches.Type(TextQueryType.Phrase |> Nullable).Operator(Nest.Operator.And |> Nullable)
                    | PhrasePrefix -> matches.Type(TextQueryType.PhrasePrefix |> Nullable).Operator(Nest.Operator.And |> Nullable)
                | Or -> 
                    match multiMatch.Type with
                    | BestFields -> matches.Type(TextQueryType.BestFields |> Nullable).Operator(Nest.Operator.Or |> Nullable)
                    | CrossFields -> matches.Type(TextQueryType.CrossFields |> Nullable).Operator(Nest.Operator.Or |> Nullable)
                    | MostFields -> matches.Type(TextQueryType.MostFields |> Nullable).Operator(Nest.Operator.Or |> Nullable)
                    | Phrase -> matches.Type(TextQueryType.Phrase |> Nullable).Operator(Nest.Operator.Or |> Nullable)
                    | PhrasePrefix -> matches.Type(TextQueryType.PhrasePrefix |> Nullable).Operator(Nest.Operator.Or |> Nullable)
            | None ->
                match multiMatch.Type with
                | BestFields -> matches.Type(TextQueryType.BestFields |> Nullable)
                | CrossFields -> matches.Type(TextQueryType.CrossFields |> Nullable)
                | MostFields -> matches.Type(TextQueryType.MostFields |> Nullable)
                | Phrase -> matches.Type(TextQueryType.Phrase |> Nullable)
                | PhrasePrefix -> matches.Type(TextQueryType.PhrasePrefix |> Nullable)
            :> IMultiMatchQuery)
        | CommonTerms commonTerms ->
            search.CommonTerms(fun c -> 
                let terms = c.Field(field commonTerms.Field).Query(commonTerms.Query).CutoffFrequency(commonTerms.CutoffFrequency |> toNullable)
                match commonTerms.MinimumShouldMatch with
                | Some min ->
                    match commonTerms.LowFrequencyOperator with
                    | Some operator ->
                        match operator with
                        | And -> terms.MinimumShouldMatch(minimumShouldMatch min).LowFrequencyOperator(Nest.Operator.And |> Nullable)
                        | Or -> terms.MinimumShouldMatch(minimumShouldMatch min).LowFrequencyOperator(Nest.Operator.Or |> Nullable)
                    | None -> terms.MinimumShouldMatch(minimumShouldMatch min)
                | None ->
                    match commonTerms.LowFrequencyOperator with
                    | Some operator ->
                        match operator with
                        | And -> terms.LowFrequencyOperator(Nest.Operator.And |> Nullable)
                        | Or -> terms.LowFrequencyOperator(Nest.Operator.Or |> Nullable)
                    | None -> terms
                :> ICommonTermsQuery)
        | QueryString queryString -> search.QueryString(fun qs -> qs |> applyQueryStringToDescriptor queryString)
        | SimpleQueryString simpleQuery -> search.SimpleQueryString(fun s -> s.Fields(fields simpleQuery.Fields).Query(simpleQuery.Query).Flags(simpleQuery.Flags |> toFlags |> Nullable) :> ISimpleQueryStringQuery)
        | Term term -> search.Term(field term.Field, term.Value, term.Boost |> toNullable)
        | Terms terms -> search.Terms(fun t -> t.Field(field terms.Field).Terms(terms.Values) :> ITermsQuery)
        | TermsSet set -> search.Terms(fun t -> t.Field(field set.Field).Terms(set.Terms) :> ITermsQuery)
        | DateRangeQuery range -> search.DateRange(fun d -> 
            let dateRange = d.Field(field range.Field).Boost(range.Boost |> toNullable)
            let ranged = 
                match range.LowerRange with
                | UnboundedLower -> 
                    match range.UpperRange with
                    | UnboundedUpper -> dateRange
                    | LessThan upper -> dateRange.LessThan(date upper)
                    | LessThanOrEqual upper -> dateRange.LessThanOrEquals(date upper)
                | GreaterThan lower ->
                    match range.UpperRange with
                    | UnboundedUpper -> dateRange.GreaterThan(date lower)
                    | LessThan upper -> dateRange.GreaterThan(date lower).LessThan(date upper)
                    | LessThanOrEqual upper -> dateRange.GreaterThan(date lower).LessThanOrEquals(date upper)
                | GreaterThanOrEqual lower ->
                    match range.UpperRange with
                    | UnboundedUpper -> dateRange.GreaterThanOrEquals(date lower)
                    | LessThan upper -> dateRange.GreaterThanOrEquals(date lower).LessThan(date upper)
                    | LessThanOrEqual upper -> dateRange.GreaterThanOrEquals(date lower).LessThanOrEquals(date upper)
            match range.Format with
            | Some format ->
                match range.TimeZone with
                | Some timeZone -> ranged.Format(format).TimeZone(timeZone)
                | None -> ranged.Format(format)
            | None ->
                match range.TimeZone with
                | Some timeZone -> ranged.TimeZone(timeZone)
                | None -> ranged
            :> IDateRangeQuery)
        | NumericRangeQuery range -> search.Range(fun d -> 
            let numRange = d.Field(field range.Field).Boost(range.Boost |> toNullable)
            match range.LowerRange with
            | UnboundedLower -> 
                match range.UpperRange with
                | UnboundedUpper -> numRange
                | LessThan upper -> numRange.LessThan(Nullable upper)
                | LessThanOrEqual upper -> numRange.LessThanOrEquals(Nullable upper)
            | GreaterThan lower ->
                match range.UpperRange with
                | UnboundedUpper -> numRange.GreaterThan(Nullable lower)
                | LessThan upper -> numRange.GreaterThan(Nullable lower).LessThan(Nullable upper)
                | LessThanOrEqual upper -> numRange.GreaterThan(Nullable lower).LessThanOrEquals(Nullable upper)
            | GreaterThanOrEqual lower ->
                match range.UpperRange with
                | UnboundedUpper -> numRange.GreaterThanOrEquals(Nullable lower)
                | LessThan upper -> numRange.GreaterThanOrEquals(Nullable lower).LessThan(Nullable upper)
                | LessThanOrEqual upper -> numRange.GreaterThanOrEquals(Nullable lower).LessThanOrEquals(Nullable upper)
            :> INumericRangeQuery)
        | StringRangeQuery range -> search.TermRange(fun d ->
            let termRange = d.Field(field range.Field).Boost(range.Boost |> toNullable)
            match range.LowerRange with
            | UnboundedLower -> 
                match range.UpperRange with
                | UnboundedUpper -> termRange
                | LessThan upper -> termRange.LessThan(upper)
                | LessThanOrEqual upper -> termRange.LessThanOrEquals(upper)
            | GreaterThan lower ->
                match range.UpperRange with
                | UnboundedUpper -> termRange.GreaterThan(lower)
                | LessThan upper -> termRange.GreaterThan(lower).LessThan(upper)
                | LessThanOrEqual upper -> termRange.GreaterThan(lower).LessThanOrEquals(upper)
            | GreaterThanOrEqual lower ->
                match range.UpperRange with
                | UnboundedUpper -> termRange.GreaterThanOrEquals(lower)
                | LessThan upper -> termRange.GreaterThanOrEquals(lower).LessThan(upper)
                | LessThanOrEqual upper -> termRange.GreaterThanOrEquals(lower).LessThanOrEquals(upper)
            :> ITermRangeQuery)                    
        | HasValue f -> search.Exists(fun e -> e.Field(field f) :> IExistsQuery)
        | Prefix prefix -> search.Prefix(fun p -> p.Field(field prefix.Field).Value(prefix.Prefix).Boost(prefix.Boost |> toNullable) :> IPrefixQuery)
        | Wildcard wildcard -> search.Wildcard(fun w -> w.Field(field wildcard.Field).Value(wildcard.Pattern).Boost(wildcard.Boost |> toNullable) :> IWildcardQuery)
        | Regexp regex -> search.Regexp(fun r -> 
            let inline reduceFlags (flags: RegexFlags list) =
                match flags with
                | [] -> String.Empty
                | head::tail -> tail |> Seq.fold (fun acc cur -> sprintf "%s|%s" acc <| getCaseName cur) (getCaseName head)
            let regexQuery = r.Field(field regex.Field).Value(regex.Regex).Boost(regex.Boost |> toNullable)
            let regexQueryWithFlags = if regex.Flags.IsEmpty
                                        then regexQuery
                                        else regexQuery.Flags(regex.Flags |> reduceFlags)
            match regex.MaxDeterminizedStates with
            | Some max -> regexQueryWithFlags.MaximumDeterminizedStates(max)
            | None -> regexQueryWithFlags
            :> IRegexpQuery)
        | Fuzzy fuzzy -> search.Fuzzy(fun f -> 
            let fuzz = f.Field(field fuzzy.Field).Boost(fuzzy.Boost |> toNullable).MaxExpansions(fuzzy.MaxExpansions |> toNullable).PrefixLength(fuzzy.PrefixLength |> toNullable).Value(fuzzy.Value)
            match fuzzy.Fuzziness with
            | Some fuzziness -> fuzz.Fuzziness(Fuzziness.EditDistance fuzziness)
            | None -> fuzz
            :> IFuzzyQuery)
        | TypeQuery t -> search.Type(fun s -> s.Value(typeName t) :> ITypeQuery)
        | Ids ids -> search.Ids(fun i -> i.Types(typeName ids.Type).Values(ids.Values |> Seq.map (fun id -> id.ToId())) :> IIdsQuery)
        | ConstantScore (query,boost) -> search.ConstantScore(fun c -> c.Boost(boost |> float |> Nullable) :> IConstantScoreQuery)
        | Bool boolQuery -> search.Bool(fun b -> 
            let musts =
                match boolQuery.Must with
                | [] -> b
                | m -> b.Must(m |> Seq.map (fun q -> applyQueryClauseToDescriptor q search) |> Seq.toArray)
            let shoulds =
                match boolQuery.Should with
                | [] -> musts
                | s -> musts.Should(s |> Seq.map (fun q -> applyQueryClauseToDescriptor q search) |> Seq.toArray)
            let mustNots =
                match boolQuery.MustNot with
                | [] -> shoulds
                | n -> shoulds.MustNot(n |> Seq.map (fun q -> applyQueryClauseToDescriptor q search) |> Seq.toArray)
            match boolQuery.Filter with
            | [] -> mustNots
            | f -> mustNots.Filter(f |> Seq.map (fun q -> applyQueryClauseToDescriptor q search) |> Seq.toArray)
            :> IBoolQuery)
        | DisMax disMax -> search.DisMax(fun d -> d.Boost(disMax.Boost |> toNullable).TieBreaker(disMax.TieBreaker |> toNullable).Queries(disMax.Queries |> Seq.map (fun q -> applyQueryClauseToDescriptor q search) |> Seq.toArray) :> IDisMaxQuery)
        | FunctionScore functionScore -> search.FunctionScore(fun f -> f.Query(fun q -> applyQueryClauseToDescriptor functionScore.Query q).MinScore(functionScore.MinScore |> toNullable).MaxBoost(functionScore.MaxBoost |> toNullable).ScoreMode(functionScore.ScoreMode.ToFunctionScoreMode() |> Nullable).BoostMode(functionScore.BoostMode.ToApi() |> Nullable) :> IFunctionScoreQuery)
        | Boosting boosting -> search.Boosting(fun b -> b.Negative(fun n -> applyQueryClauseToDescriptor boosting.Negative n).Positive(fun p -> applyQueryClauseToDescriptor boosting.Positive p).NegativeBoost(boosting.NegativeBoost |> toNullable) :> IBoostingQuery)
        | Nested nested -> search.Nested(fun n -> n.Path(field nested.Path).Query(fun q -> applyQueryClauseToDescriptor nested.Query q).ScoreMode(nested.ScoreMode.ToNested()) :> INestedQuery)
        | HasChild hasChild -> search.HasChild(fun c -> c.Query(fun q -> applyQueryClauseToDescriptor hasChild.Query q).Type(hasChild.Type) :> IHasChildQuery)
        | HasParent hasParent -> search.HasParent(fun p -> p.Query(fun q -> applyQueryClauseToDescriptor hasParent.Query q).Type(hasParent.ParentType) :> IHasParentQuery)
        | ParentId parentId -> search.ParentId(fun p -> p.Id(parentId.Id.ToId()).Type(typeName parentId.Type) :> IParentIdQuery)

    let inline applySearchRequestToDescriptor<'index when 'index: not struct> (request: CurryOn.Elastic.SearchRequest) (search: SearchDescriptor<'index>) =
        let inline getSearchBody (structure: SearchStructure) (search: SearchDescriptor<'index>) =
            match structure with
            | QueryStringQuery queryString ->
                search.Query(fun qd -> qd.QueryString(fun qs -> qs |> applyQueryStringToDescriptor queryString))                    
            | RequestBodyQuery requestBody -> 
                search.Query(fun qd -> qd |> applyQueryClauseToDescriptor requestBody)
                    
        let rec applySort sort (descriptor: SearchDescriptor<'index>) =
            let inline getSortPromise (sort: SortDescriptor<'index>) =
                sort :> IPromise<Collections.Generic.IList<ISort>>
            match sort with
            | Field f -> descriptor.Sort(fun sd -> sd.Field(field f, SortOrder.Ascending) |> getSortPromise)
            | FieldDirection (f,direction) ->
                match direction with
                | Ascending -> descriptor.Sort(fun sd -> sd.Ascending(field f) |> getSortPromise)
                | Descending -> descriptor.Sort(fun sd -> sd.Descending(field f) |> getSortPromise)
            | Multiple sorts -> sorts |> Seq.fold (fun acc cur -> applySort cur acc) descriptor
            | Score -> descriptor.Sort(fun sd -> sd.Descending(SortSpecialField.Score) |> getSortPromise)
            | Document -> descriptor.Sort(fun sd -> sd.Ascending(SortSpecialField.DocumentIndexOrder) |> getSortPromise)

        let searchBody = getSearchBody request.Search search

        let finalizedSearch =
            match request.From with
            | Some from ->
                match request.Size with
                | Some size ->
                    match request.Sort with
                    | Some sort ->
                        match request.Timeout with
                        | Some timeout -> searchBody.From(from).Size(size).Timeout(sprintf "%fs" timeout.TotalSeconds) |> applySort sort
                        | None -> searchBody.From(from).Size(size) |> applySort sort
                    | None ->
                        match request.Timeout with
                        | Some timeout -> searchBody.From(from).Size(size).Timeout(sprintf "%fs" timeout.TotalSeconds)
                        | None -> searchBody.From(from).Size(size)
                | None ->
                    match request.Sort with
                    | Some sort ->
                        match request.Timeout with
                        | Some timeout -> searchBody.From(from).Timeout(sprintf "%fs" timeout.TotalSeconds) |> applySort sort
                        | None -> searchBody.From(from) |> applySort sort
                    | None ->
                        match request.Timeout with
                        | Some timeout -> searchBody.From(from).Timeout(sprintf "%fs" timeout.TotalSeconds)
                        | None -> searchBody.From(from)
            | None -> 
                match request.Size with
                | Some size ->
                    match request.Sort with
                    | Some sort ->
                        match request.Timeout with
                        | Some timeout -> searchBody.Size(size).Timeout(sprintf "%fs" timeout.TotalSeconds) |> applySort sort
                        | None -> searchBody.Size(size) |> applySort sort
                    | None ->
                        match request.Timeout with
                        | Some timeout -> searchBody.Size(size).Timeout(sprintf "%fs" timeout.TotalSeconds)
                        | None -> searchBody.Size(size)
                | None ->
                    match request.Sort with
                    | Some sort ->
                        match request.Timeout with
                        | Some timeout -> searchBody.Timeout(sprintf "%fs" timeout.TotalSeconds) |> applySort sort
                        | None -> searchBody |> applySort sort
                    | None ->
                        match request.Timeout with
                        | Some timeout -> searchBody.Timeout(sprintf "%fs" timeout.TotalSeconds)
                        | None -> searchBody

        finalizedSearch :> ISearchRequest

    let inline applySearchToDeleteByQueryDescriptor<'index when 'index: not struct> (search: CurryOn.Elastic.SearchRequest) (delete: DeleteByQueryDescriptor<'index>) =
        let deleteQuery =
            match search.Search with
            | QueryStringQuery queryString ->
                delete.Query(fun qd -> qd.QueryString(fun qs -> qs |> applyQueryStringToDescriptor queryString))                    
            | RequestBodyQuery requestBody -> 
                delete.Query(fun qd -> qd |> applyQueryClauseToDescriptor requestBody)
        
        let rec getSort sort =
            match sort with
            | Field f -> sprintf "%s:asc" f
            | FieldDirection (f,direction) ->
                match direction with
                | Ascending -> sprintf "%s:asc" f
                | Descending -> sprintf "%s:desc" f
            | Multiple sorts -> 
                match sorts with
                | [] -> String.Empty
                | [s] -> getSort s
                | head::tail -> tail |> Seq.fold (fun acc cur -> sprintf "%s,%s" acc (getSort cur)) (getSort head)
            | Score -> "_score:desc"
            | Document -> "_doc:asc"

        let finalizedDelete =
            match search.From with
            | Some from ->
                match search.Size with
                | Some size ->
                    match search.Sort with
                    | Some sort ->
                        match search.Timeout with
                        | Some timeout -> deleteQuery.From(from |> int64).Size(size |> int64).Timeout(time timeout).Sort(getSort sort)
                        | None -> deleteQuery.From(from |> int64).Size(size |> int64).Sort(getSort sort)
                    | None ->
                        match search.Timeout with
                        | Some timeout -> deleteQuery.From(from |> int64).Size(size |> int64).Timeout(time timeout)
                        | None -> deleteQuery.From(from |> int64).Size(size |> int64)
                | None ->
                    match search.Sort with
                    | Some sort ->
                        match search.Timeout with
                        | Some timeout -> deleteQuery.From(from |> int64).Timeout(time timeout).Sort(getSort sort)
                        | None -> deleteQuery.From(from |> int64).Sort(getSort sort)
                    | None ->
                        match search.Timeout with
                        | Some timeout -> deleteQuery.From(from |> int64).Timeout(time timeout)
                        | None -> deleteQuery.From(from |> int64)
            | None -> 
                match search.Size with
                | Some size ->
                    match search.Sort with
                    | Some sort ->
                        match search.Timeout with
                        | Some timeout -> deleteQuery.Size(size |> int64).Timeout(time timeout).Sort(getSort sort)
                        | None -> deleteQuery.Size(size |> int64).Sort(getSort sort)
                    | None ->
                        match search.Timeout with
                        | Some timeout -> deleteQuery.Size(size |> int64).Timeout(time timeout)
                        | None -> deleteQuery.Size(size |> int64)
                | None ->
                    match search.Sort with
                    | Some sort ->
                        match search.Timeout with
                        | Some timeout -> deleteQuery.Timeout(time timeout).Sort(getSort sort)
                        | None -> deleteQuery.Sort(getSort sort)
                    | None ->
                        match search.Timeout with
                        | Some timeout -> deleteQuery.Timeout(time timeout)
                        | None -> deleteQuery

        finalizedDelete :> IDeleteByQueryRequest

    let search<'index when 'index: not struct> (client: ElasticClient) (request: CurryOn.Elastic.SearchRequest) =
        operation {
            let! response = client.SearchAsync<'index>(fun sd -> sd |> applySearchRequestToDescriptor request)
            return! if response.IsValid
                    then let searchResponse =
                            { ElapsedTime = response.Took |> float |> TimeSpan.FromMilliseconds
                              TimedOut = response.TimedOut
                              Shards = {Total = response.Shards.Total; Failed = response.Shards.Failed; Successful = response.Shards.Successful}
                              Results = { TotalHits = response.HitsMetaData.Total
                                          MaximumScore = Some response.HitsMetaData.MaxScore
                                          Hits = response.Hits 
                                                 |> Seq.map (fun hit -> {Index = hit.Index; Type = hit.Type; Id = hit.Id |> DocumentId.Parse; Score = hit.Score |> toOption; Document = hit.Source})
                                                 |> Seq.toList
                                        }
                              ScrollId = response.ScrollId
                            }
                         Result.successWithEvents searchResponse [SearchExecutedSuccessfully]
                    else Result.failure [response |> toError |> QueryExecutionError]
        }

    let deleteByQuery<'index when 'index: not struct> (client: ElasticClient) (search: CurryOn.Elastic.SearchRequest) =
        operation {
            let! response = client.DeleteByQueryAsync(fun d -> d |> applySearchToDeleteByQueryDescriptor search)
            return! if response.IsValid
                    then let deleteResponse = 
                            { ElapsedTime = response.Took |> float |> TimeSpan.FromMilliseconds
                              TimedOut = response.TimedOut
                              TotalMatchingQuery = response.Total
                              Deleted = response.Deleted
                              Failed = response.Failures.Count |> int64
                            }
                         Result.successWithEvents deleteResponse [SearchExecutedSuccessfully]
                    else Result.failure [response |> toError |> DeleteByQueryFailed]
        }

    let distinctValues<'index,'field when 'index: not struct> (client: ElasticClient) (request: DistinctValuesRequest<'field>) =
        operation {
            let name = sprintf "distinct_%s_agg" request.Field
            let size = if request.Size.IsSome then request.Size.Value else 0
            let! response = client.SearchAsync<'index>(fun (s: SearchDescriptor<'index>) -> 
                s.Aggregations(fun a -> a.Terms(name, fun t -> 
                    t.Field(field request.Field).Size(size) :> ITermsAggregation) :> IAggregationContainer) :> ISearchRequest)
            return! if response.IsValid
                    then let distinctResponse =
                            { ElapsedTime = response.Took |> float |> TimeSpan.FromMilliseconds
                              TimedOut = response.TimedOut
                              Shards = {Successful = response.Shards.Successful; Failed = response.Shards.Failed; Total = response.Shards.Total}
                              Results = { TotalHits = response.HitsMetaData.Total
                                          MaximumScore = Some response.HitsMetaData.MaxScore
                                          Hits = response.Hits 
                                                 |> Seq.map (fun hit -> {Index = hit.Index; Type = hit.Type; Id = hit.Id |> DocumentId.Parse; Score = hit.Score |> toOption; Document = hit.Source})
                                                 |> Seq.toList
                                        }
                              Aggregations = response.Aggs.Terms(name).Buckets |> Seq.map (fun bucket -> {Key = bucket.Key |> unbox<'field>; DocumentCount = bucket.DocCount.GetValueOrDefault()}) |> Seq.toList
                            }
                         Result.successWithEvents distinctResponse [AggregateQueryExecuted]
                    else Result.failure [response |> toError |> AggregateQueryFailed]
        }