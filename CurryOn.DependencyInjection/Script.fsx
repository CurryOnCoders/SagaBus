#r "System.Configuration"
#r "bin/debug/CurryOn.Common.dll"
#r "bin/debug/CurryOn.DependencyInjection.dll"

open System
open CurryOn.Common
open CurryOn.Core

type TestComponent () =
    interface IComponent

let test = Context.Container.InstanceOf<IComponent>()

