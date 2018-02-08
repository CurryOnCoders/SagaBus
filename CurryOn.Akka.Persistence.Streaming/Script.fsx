// Learn more about F# at http://fsharp.org. See the 'F# Tutorial' project
// for more guidance on F# programming.

type EventHandler = delegate of obj*System.EventArgs -> unit

let f (p: EventHandler, s: string) = p.Invoke(null, null)

f (EventHandler (fun sender event -> printfn "Fired"), "Test")