namespace CurryOn.Common

open FSharp.Control

type Envelope<'message, 'result, 'event> (message: 'message) =
    let resultCell = ref Unchecked.defaultof<OperationResult<'result,'event>>
    let hasResult = ref false
    let resultSet

