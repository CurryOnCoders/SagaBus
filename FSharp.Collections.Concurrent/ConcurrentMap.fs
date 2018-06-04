namespace FSharp.Collections.Concurrent

open System.Collections.Concurrent 

type ConcurrentMap<'key,'value> = ConcurrentDictionary<'key,'value>

module ConcurrentMap =
    open System.Collections.Generic

    let empty<'key,'value> = ConcurrentMap<'key,'value>()

    let inline addOrUpdate key value (map: ConcurrentMap<_,_>) =
        map.AddOrUpdate(key, value, fun _ _ -> value)

    let inline addOrUpdateWith key value computeValue (map: ConcurrentMap<_,_>) =
        map.AddOrUpdate(key, value, fun k v -> computeValue k v)

    let inline clear (map: ConcurrentMap<_,_>) =
        map.Clear()
        map

    let inline containsKey key (map: ConcurrentMap<_,_>) =
        map.ContainsKey(key)

    let inline count (map: ConcurrentMap<_,_>) =
        map.Count
    
    let inline getOrAdd key (value: 'b) (map: ConcurrentMap<_,_>) =
        map.GetOrAdd(key, value)

    let inline isEmpty (map: ConcurrentMap<_,_>) =
        map.IsEmpty

    let inline keys (map: ConcurrentMap<_,_>) =
        map.Keys

    let inline values (map: ConcurrentMap<_,_>) =
        map.Values

    let inline tryGetValue key (map: ConcurrentMap<_,_>) =
        match map.TryGetValue(key) with
        | (true, value) -> Some value
        | _ -> None

    let inline tryRemove key (map: ConcurrentMap<_,_>) =
        match map.TryRemove(key) with
        | (true, value) -> Some value
        | _ -> None

    let inline tryUpdate key newValue oldValue (map: ConcurrentMap<_,_>) =
        map.TryUpdate(key, newValue, oldValue)

    let inline toSeq (map: ConcurrentMap<_,_>) =
        seq { for kvp in map -> kvp.Key, kvp.Value }
    
    let toList<'key,'value> = toSeq >> Seq.toList<'key*'value>

    let toArray<'key,'value> = toSeq >> Seq.toArray<'key*'value>

    let inline ofSeq items =
        ConcurrentDictionary(items |> Seq.map (fun (key, value) -> KeyValuePair(key, value)))
    
    let ofList<'key,'value> : List<'key*'value> -> ConcurrentDictionary<'key,'value> = ofSeq

    let ofArray<'key,'value> : ('key*'value) [] -> ConcurrentDictionary<'key,'value> = ofSeq
    