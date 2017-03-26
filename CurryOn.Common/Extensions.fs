namespace CurryOn.Common

open FSharp.Quotations
open FSharp.Quotations.Patterns
open FSharp.Quotations.Evaluator
open System
open System.Threading.Tasks

[<AutoOpen>]
module Extensions =

    type Task with
        member task.IsSuccessful = task.IsCompleted && (not task.IsFaulted)

    type Type with
        member this.IsEquivalentTo (other: Type) = other.IsAssignableFrom(this)
        member this.AggregateName = this.Name |> AggregateName.New

    type Collections.Generic.IDictionary<'k,'v> with
        member this.GetValue key =
            let item = ref Unchecked.defaultof<'v>
            if this.TryGetValue(key, item)
            then Some item.Value
            else None

    let private replace (oldValue: String) newValue (str: String) =
        str.Replace(oldValue, newValue)

    type String with
        static member Replace oldValue newValue str =
            str |> replace oldValue newValue

    type DateTime with
        member this.ToExpiryDate formatString =
            sprintf formatString this.Month this.Year
            
    type Option<'T> with
        member this.ValueOrDefault = 
            match this with
            | Some value -> value
            | None -> Unchecked.defaultof<'T>

    type Object with
        member this.Copy<'t when 't : (new: unit -> 't)> ([<ReflectedDefinition>] assignmentExpressions: Expr list) =
            let copyType = typeof<'t>
            let newInstance = Activator.CreateInstance(copyType)
            let fields = copyType.GetProperties()
            for field in fields do
                let exprValue = 
                    assignmentExpressions 
                    |> List.map (fun expr ->
                        match expr with
                        | PropertySet (instance, propertyInfo, arguments, valueExpr) -> 
                            if propertyInfo = field then
                                let value = QuotationEvaluator.EvaluateUntyped valueExpr
                                Some value
                            else None
                        | e -> None)
                    |> List.tryFind Option.isSome
                match exprValue with
                | Some valueOption ->
                    match valueOption with
                    | Some value -> field.SetValue(newInstance, value)
                    | None -> field.SetValue(newInstance, field.GetValue(this))
                | None -> field.SetValue(newInstance, field.GetValue(this))
            newInstance |> unbox<'t>

    type FSharp.Reflection.UnionCaseInfo with
        member private this.LazyFields = lazy(this.GetFields())
        member this.Fields = this.LazyFields.Force()
        member this.HasFields = 
            try
                let fields = this.LazyFields.Force()
                fields <> null && fields.Length > 0
            with | _ -> false

module String =
    let uppercase (s: string) = s.ToUpper()
    let lowercase (s: string) = s.ToLower()
    let replace find repl (s: string) = s |> String.Replace find repl 


module Unchecked =
    let private defaultMethod = lazy(
        match <@@ Unchecked.defaultof<int> @@> with 
        | Quotations.Patterns.Call(_,minfo,_) -> minfo.GetGenericMethodDefinition()
        | _ -> failwith "Unexpected failure initializing Unchecked extension module")
        
    let defaultOfType (objectType: Type) =
        defaultMethod.Value.MakeGenericMethod([|objectType|]).Invoke(null, [||])


module Seq =
    /// Maps each item in the sequence with the given mapper function inside a try/with block and returns only the items that succeed
    let tryMap mapper items =
        items |> Seq.map (fun item -> try item |> mapper |> Some with | _ -> None)
              |> Seq.filter (fun result -> result.IsSome)
              |> Seq.map (fun result -> result.Value)