namespace CurryOn.Common

open FSharp.Control
open System
open System.Configuration

type IConfiguration =
    abstract member AssemblySearchPath: string

module Configuration =
    type ContextConfigurationSection () =
        inherit ConfigurationSection()
        static member internal SectionName = "curryOn.sagaBus"
        [<ConfigurationProperty("assemblySearchPath", IsRequired = false, DefaultValue = @".\")>]
        member this.AssemblySearchPath    
            with get() = this.["assemblySearchPath"].ToString()
            and set (value: string) = this.["assemblySearchPath"] <- value
        interface IConfiguration with
            member this.AssemblySearchPath = this.AssemblySearchPath

    let SectionName = ContextConfigurationSection.SectionName
    let load () =
        operation {
            return ConfigurationManager.GetSection(SectionName) |> unbox<IConfiguration>
        }
    let Common = 
        match load() |> Operation.wait with
        | Success success -> success.Result
        | Failure _ -> new ContextConfigurationSection() :> IConfiguration
