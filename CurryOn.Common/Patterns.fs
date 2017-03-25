namespace CurryOn.Common

open System
open System.Text.RegularExpressions

module Patterns =
    let (|String|_|): obj -> String option = function
    | :? string as str -> Some str
    | :? IConvertible as ic -> Convert.ToString(ic) |> Some
    | _ -> None

    let (|Int32|_|): obj -> int option = function
    | :? Int32 as i -> Some i
    | String str ->
        try Some (Int32.Parse(str))
        with _ -> None
    | _ -> None

    let (|Int64|_|): obj -> int64 option = function
    | :? Int64 as i -> Some i
    | String str ->
        try Some (Int64.Parse(str))
        with _ -> None
    | _ -> None

    let (|DateTime|_|): obj -> DateTime option = function
    | :? DateTime as i -> Some i
    | String str ->
        try Some (DateTime.Parse(str))
        with _ -> None
    | _ -> None

    let (|Boolean|_|): obj -> bool option = function
    | :? Boolean as i -> Some i
    | String str ->
        try Some (Boolean.Parse(str))
        with _ -> None
    | _ -> None

    let (|Decimal|_|): obj -> decimal option = function
    | :? Decimal as i -> Some i
    | String str ->
        try Some (Decimal.Parse(str))
        with _ -> None
    | _ -> None

    let (|Guid|_|): obj -> Guid option = function
    | :? Guid as i -> Some i
    | String str ->
        try Some (Guid.Parse(str))
        with _ -> None
    | _ -> None

    // IComparable
    let (|Compare|_|) (pattern: IComparable<_>) (value: IComparable<_>) =
        if value.CompareTo(pattern) >= 0 then Some value else None

    // String matching
    let (|Like|_|) (pattern: string) (str: string) =
        if str |> like pattern then Some Like else None

    let (|Begins|_|) (pattern: string) (str: string) =
        if str.StartsWith(pattern) then Some str else None

    let (|Contains|_|) (pattern: string) (str: string) =
        if str.Contains(pattern) then Some str else None

    let (|Ends|_|) (pattern: string) (str: string) =
        if (str.EndsWith(pattern)) then Some str else None

    let (|Regex|_|) pattern str =
        let expression = new Regex(pattern)
        if expression.IsMatch(str) then Some str else None

    let (|RegexGroups|_|) pattern input =
        let m = Regex.Match(input, pattern)
        if m.Success then Some(List.tail [ for g in m.Groups -> g.Value ])
        else None