#r "bin/debug/CurryOn.FSharp.Control.dll"

open FSharp.Control
open System
open System.Net

// --------------------------------------
// Original Approach
let parseIpAddress (inputString: string) =
    let segments = inputString.Split('.')
    let bytes = segments |> Array.map Convert.ToByte
    IPAddress bytes

// Good: Short, simple code.  Business logic is obvious.
// Bad:  Throws exceptions!  Not what the function signature implies (string -> IPAddress).
//       parseIpAddress "127.0.1000.1" -> System.OverflowException: Value was either too large or too small for an unsigned byte.     
//       Can return at least 4 different exceptions (ArgumentException, OverflowException, NullReferenceException, NumberFormatException).
//       Usefulness of Error Message depends on which exception is thrown.



// -------------------------------------------
// First Refinment:  Maybe Monad / "Try Parse"
let parseIpAddress (inputString: string) =
    try let segments = inputString.Split('.')
        let bytes = segments |> Array.map Convert.ToByte
        bytes |> IPAddress |> Some
    with | _ ->
        None

// Good: Still short and simple, doesn't throw exceptions, so the signature is accurate (string -> IPAddress option)
// Bad:  No information about why it didn't work.  All failures are identical -> None
//       parseIpAddress "127.0.1000.1" -> None:  The error is swallowed and we're left with nothing



// ------------------------------------------------
// Standard Railway-Oriented Approach (Either Monad)
type IPValidationEvent =
| IpAddressValidated
| InputStringWasNullOrEmpty
| SegmentNotNumerical of string
| SegmentOutOfRange of string
| InvalidNumberOfSegments of int

let validateNotNull s =
    if String.IsNullOrWhiteSpace s
    then Failure [InputStringWasNullOrEmpty]
    else Result.success s

let validateNumerical (s: string) =
    let isNumerical = s |> Seq.fold (fun acc cur -> acc && Char.IsDigit(cur)) true
    if isNumerical
    then Int32.Parse(s) |> Result.success
    else Failure [SegmentNotNumerical s]

let validateRange i =
    if i < 0 || i > 255
    then Failure [SegmentOutOfRange (i |> string)]
    else Result.success <| Convert.ToByte i

let validateNumberOfSegments (s: string) =
    let segments = s.Split('.')
    if segments.Length = 4
    then Result.success segments
    else Failure [InvalidNumberOfSegments segments.Length]

let validateSegments (segments: string []) =
    segments 
    |> Array.map validateNumerical 
    |> Array.map (fun n -> n >>= validateRange)
    |> Seq.fold (fun acc cur ->
        match acc with
        | Success s1 ->
            match cur with
            | Success s2 -> s2.Result::s1.Result |> Result.success
            | Failure events -> Failure (s1.Events @ events)
        | Failure events -> Failure events) (Result.success [])
    |> (fun s -> Seq.rev <!> s)
    |> (fun s -> Seq.toArray <!> s)

let parseIpAddress inputString =
    inputString
    |> validateNotNull
    >>= validateNumberOfSegments
    >>= validateSegments
    >>= (fun segments -> Result.successWithEvents (Net.IPAddress segments) [IpAddressValidated])
        
// Good: Function signature is accurate:  string -> Result<IPAddress, IPValidationEvent> (always returns the same type)
//       Descriptive Error Messages:  parseIpAddress "127.0.1000.1" -> Failure [SegmentOutOfRange 1000]
//       Some validations could be re-used
//
// Bad:  Much more verbose, original logic is obscured by mechanism
//       Relies on non-standard operators like >>= (bind railway) and <!> (lift railway)



// -----------------------------------------------------------------
// Operation Framework Approach (Computation Builder)
let getIpSegments (s: string) =
    operation {      
        return! if s |> String.IsNullOrWhiteSpace
                then Result.failure [InputStringWasNullOrEmpty]
                else let segments = s.Split('.')
                     if segments.Length = 4
                     then Result.success segments
                     else Result.failure [InvalidNumberOfSegments segments.Length]
    }

let parseIpSegment (s: string) =
    operation {
        return! if s |> Seq.forall Char.IsDigit
                then match Byte.TryParse s with
                     | (true, b) -> Result.success b
                     | _ -> Result.failure [SegmentOutOfRange s]
                else Result.failure [SegmentNotNumerical s]
    }

let parseIpAddress inputString = 
    operation {
        let! segments = inputString |> getIpSegments
        let! bytes = segments |> Array.map parseIpSegment |> Operation.join
        return! Result.successWithEvents (IPAddress bytes) [IpAddressValidated]
    }

// Good: Function signature is accurate:  string -> Result<IPAddress, IPValidationEvent> (always returns the same type)
//       Descriptive Error Messages:  parseIpAddress "127.0.1000.1" -> Failure [SegmentOutOfRange 1000]
//       Not as verbose, original business logic is preserved
//       Makes use of standard F# syntax for Computation Expressions (let!, return!, etc.)
//
// Bad:  Validations are less modular