namespace CurryOn.Common

open System

type TransparentMeasureAttribute(typeCode: TypeCode) =
    inherit Attribute()
    member __.TypeCode = typeCode

// Define Measures for Non-Numeric Types
[<TransparentMeasure(TypeCode.Boolean)>]
[<CLIMutable>]
type bool<[<Measure>] 'U> = { Value: bool } with 
    static member Empty : bool<'U> = {Value = false}
    static member (<=>) (b1: bool<'u>, b2: bool) = b1.Value = b2
    static member op_Implicit (systemBool: bool) = {Value = systemBool}
   
[<TransparentMeasure(TypeCode.DateTime)>]
[<CLIMutable>]
type DateTime<[<Measure>] 'U> = { Value: DateTime } with 
    static member Empty : DateTime<'U> = {Value = DateTime.MinValue}
    static member (<=>) (d1: DateTime<'u>, d2: DateTime) = d1.Value = d2
    static member op_Implicit (systemDate: DateTime) = {Value = systemDate}
    
[<TransparentMeasure(TypeCode.String)>]
[<CLIMutable>]
type string<[<Measure>] 'U> = { Value: string } with 
    static member Empty : string<'U> = {Value = String.Empty}
    static member (<=>) (s1: string<'u>, s2: string) = s1.Value = s2
    static member op_Implicit (systemString: string) = {Value = systemString}

[<TransparentMeasure(TypeCode.String)>]
[<CLIMutable>]
type Guid<[<Measure>] 'U> = { Value: Guid } with 
    static member Empty : Guid<'U> = {Value = Guid.Empty}
    static member (<=>) (g1: Guid<'u>, g2: Guid) = g1.Value = g2
    static member op_Implicit (systemGuid: Guid) = {Value = systemGuid}

[<TransparentMeasure(TypeCode.String)>]
[<CLIMutable>]
type obj<[<Measure>] 'U> = { Value: obj } with 
    static member Empty : obj<'U> = {Value = null}
    static member (<=>) (o1: obj<'u>, o2: obj) = o1.Value = o2
    static member op_Implicit (systemObj: obj) = {Value = systemObj}

[<AutoOpen>]
module Measures =
    let inline isValidString (measureValue: string<_>) = 
        measureValue.Value |> isNotNullAndNotEmpty

    let inline isValidBool (measureValue: bool<_>) = 
        try 
            match measureValue.Value with
            | true | false -> true
        with | _ -> false

    let inline isValidDate (measureValue: DateTime<_>) = 
        measureValue.Value <> DateTime.MinValue

    let inline isValidGuid (measureValue: Guid<_>) = 
        measureValue.Value <> Guid.Empty

    let inline isValidObject (measureValue: string<_>) = 
       measureValue.Value |> isNotNullAndNotEmpty

    // Add Value member to existing Measure types
    type decimal<[<Measure>] 'U> with
        member this.Value = this |> decimal

    type int<[<Measure>] 'U> with
        member this.Value = this |> int  

    type int64<[<Measure>] 'U> with
        member this.Value = this |> int64

    type float<[<Measure>] 'U> with
        member this.Value = this |> float

    // Generates a measure type from a nullable primitive.
    let toMeasureValue<'t,'d when 't :> ValueType and 't: struct and 't: (new: unit -> 't)> (factory: 't -> 'd) (input: Nullable<'t>) = if input.HasValue then factory input.Value else factory Unchecked.defaultof<'t>

    [<Measure>] type Tenant         = static member New: string -> string<Tenant>          = (fun v -> {Value = v})
    [<Measure>] type AggregateName  = static member New: string -> string<AggregateName>   = (fun v -> {Value = v})
    [<Measure>] type AggregateKey   = static member New: string -> string<AggregateKey>    = (fun v -> {Value = v})
    [<Measure>] type EntityId       = static member New: string -> string<EntityId>        = (fun v -> {Value = v})
    [<Measure>] type version        = static member New (value: #IComparable) = value |> Convert.ToInt32 |> LanguagePrimitives.Int32WithMeasure<version>