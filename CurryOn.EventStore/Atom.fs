namespace CurryOn.Akka.EventStore

open CurryOn.Common
open CurryOn.Common.Caching
open CurryOn.Core.Serialization
open EventStore.ClientAPI
open FSharp.Control
open Reactive.Streams
open System
open System.Net.Http

type ReadOperation =
| Forward
| ForwardFrom of int64
| ForwardFor of (int64*int64)
| Backward
| BackwardFrom of int64
| BackwardFor of (int64*int64)

[<CLIMutable>]
type AtomAuthor =
    {
        Name: string;
    }

type AtomRelation =
| Self
| First
| Last
| Next
| Previous
| Metadata
| Alternate
| Edit
| Other of string

[<CLIMutable>]
type AtomLink =
    {
        Uri: Uri;
        Relation: AtomRelation;
    }

[<CLIMutable>]
type AtomEntry =
    {
        Id: Uri;
        Title: string;
        Updated: DateTime;
        StreamId: string;
        Author: AtomAuthor;
        HeadOfStream: bool;
        ETag: string;
        Links: AtomLink [];
        Entries: AtomEntry [];
    }
    member this.Stream = 
        if this.StreamId |> isNullOrEmpty
        then this.Title.Substring(this.Title.IndexOf('@') + 1)
        else this.StreamId
    member this.Category =
        this.Stream.Split('-').[0]

[<CLIMutable>]
type AtomFeed =
    {
        Entries: AtomEntry [];
    }


module Atom =
    let private MaxEntries = 4095
    let private Forward = "forward"
    let private Backward = "backward"
    let private BaseUri = ref "http://localhost:2113"

    let private httpClient =
        let client = new HttpClient()
        client.DefaultRequestHeaders.Accept.Add(Headers.MediaTypeWithQualityHeaderValue.Parse("application/json"))
        client

    let private baseUri = lazy(BaseUri.Value)

    let private getFeedUri feedName =
        let root = !baseUri
        sprintf "%s/streams/%s" root feedName

    let private fetch (requestUri: string) =
        async {
            let! httpResponse = httpClient.GetAsync(requestUri) |> Async.AwaitTask
            if httpResponse.IsSuccessStatusCode then 
                let! responseText = httpResponse.Content.ReadAsStringAsync() |> Async.AwaitTask
                return responseText |> parseJson<AtomFeed> 
            else 
                return {Entries = [||]}
        }

    let setBaseUri uri = BaseUri := uri

    let fetchAtom = 
        let cache = new Cache<string,AtomFeed>()
        fun feedUri ->
            let fetchFresh () =               
                let feed = fetch feedUri |> Async.RunSynchronously        
                cache.add feedUri feed
                feed
            if cache.has feedUri
            then
                let feed = cache.get feedUri
                if feed.Entries.Length = MaxEntries
                then feed
                else fetchFresh ()           
            else fetchFresh ()        

    let getFeed feedName =
        fetchAtom <| getFeedUri feedName

    let private getEntries feedName from direction count =
        let rec readAtomFeed feedName (from: int64) direction (count: int64) =
            seq {
                let requestUri = sprintf "%s/%d/%s/%d" (getFeedUri feedName) from direction (Math.Min(MaxEntries |> int64, count))
                let feed = fetchAtom requestUri
                if feed.Entries.Length > 0 then
                    yield feed
                    yield! readAtomFeed feedName (from + (feed.Entries.Length |> int64)) direction (count - (feed.Entries.Length |> int64))
            }
        {new IPublisher<AtomFeed> with
            member __.Subscribe subscriber =
                readAtomFeed feedName from direction count |> Seq.iter subscriber.OnNext            
        }  
                
    let rec private getAllEntries feedName from direction =
        let rec readAllEntries feedName (from: int64) direction =
            seq {
                let requestUri = sprintf "%s/%d/%s/%d" (getFeedUri feedName) from direction MaxEntries
                let feed = fetchAtom requestUri
                if feed.Entries.Length > 0 then
                    yield feed
                    yield! readAllEntries feedName (from + (feed.Entries.Length |> int64)) direction            
            } 
        {new IPublisher<AtomFeed> with
            member __.Subscribe subscriber =
                readAllEntries feedName from direction |> Seq.iter subscriber.OnNext            
        }

    let readFeed feedName = function
    | Forward -> getAllEntries feedName 0L Forward
    | ForwardFrom start -> getAllEntries feedName start Forward
    | ForwardFor (start,limit) -> getEntries feedName start Forward limit
    | Backward -> getAllEntries feedName 0L Backward
    | BackwardFrom start -> getAllEntries feedName start Backward
    | BackwardFor (start,limit) -> getEntries feedName start Backward limit