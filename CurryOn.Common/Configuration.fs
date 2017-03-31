namespace CurryOn.Common

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
        attempt {
            return ConfigurationManager.GetSection(SectionName) |> unbox<IConfiguration>
        }
    let Common = 
        match load() with
        | Success config -> config
        | Failure _ -> new ContextConfigurationSection() :> IConfiguration
