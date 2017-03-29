namespace CurryOn.Core

open CurryOn.Common
open System.Configuration

type IAggregateAgentConfiguration =
    abstract member AggregateName: string<AggregateName>
    abstract member NumberOfAgents: int

type IAgentConfiguration =
    abstract member NumberOfSharedAgents: int
    abstract member AggregateAgents: IAggregateAgentConfiguration seq

type IBusConfiguration =
    inherit IConfiguration
    abstract member Agents: IAgentConfiguration

module Configuration =
    type AggregateAgentConfigurationElement () =
        inherit ConfigurationElement()
        [<ConfigurationProperty("aggregate", IsRequired = true)>]
        member this.AggregateName
            with get () = this.["aggregate"].ToString()
            and set (value: string) = this.["aggregate"] <- value
        [<ConfigurationProperty("numberOfAgents", IsRequired = false, DefaultValue = 10)>]
        member this.NumberOfAgents
            with get () = this.["numberOfAgents"] :?> int
            and set (value: int) = this.["numberOfAgents"] <- value
        interface IAggregateAgentConfiguration with
            member this.AggregateName = this.AggregateName |> AggregateName.New
            member this.NumberOfAgents = this.NumberOfAgents


    [<ConfigurationCollectionAttribute(typeof<AggregateAgentConfigurationElement>, AddItemName = "aggregateAgents")>]
    type AggregateAgentConfigurationElementCollection () =  
        inherit ConfigurationElementCollection()
        override this.CreateNewElement () = AggregateAgentConfigurationElement() |> unbox<ConfigurationElement>
        override this.GetElementKey (element: ConfigurationElement) =  (element |> unbox<AggregateAgentConfigurationElement>).AggregateName |> box
        interface IAggregateAgentConfiguration seq with
            member this.GetEnumerator () = (this |> Seq.cast<IAggregateAgentConfiguration>).GetEnumerator()

    type AgentConfigurationElement () =
        inherit ConfigurationElement()
        [<ConfigurationProperty("numberOfSharedAgents", IsRequired = false, DefaultValue = 100)>]
        member this.NumberOfAgents
            with get () = this.["numberOfSharedAgents"] :?> int
            and set (value: int) = this.["numberOfSharedAgents"] <- value
        [<ConfigurationProperty("aggregateAgents")>]
        member this.AggregateAgents 
            with get () = this.["aggregateAgents"] :?> AggregateAgentConfigurationElementCollection
            and set (value: AggregateAgentConfigurationElementCollection) = this.["aggregateAgents"] <- value
        interface IAgentConfiguration with
            member this.NumberOfSharedAgents = this.NumberOfAgents
            member this.AggregateAgents = this.AggregateAgents |> unbox<IAggregateAgentConfiguration seq>

    type BusConfigurationSection () =
        inherit Configuration.ContextConfigurationSection ()
        [<ConfigurationProperty("agents")>]
        member this.Agents 
            with get () = this.["agents"] :?> AgentConfigurationElement
            and set (value: AgentConfigurationElement) = this.["agents"] <- value
        interface IBusConfiguration with
            member this.Agents = this.Agents :> IAgentConfiguration

    let reload<'a> () =
        attempt {
            return ConfigurationManager.GetSection(Configuration.SectionName) |> unbox<'a>
        }

    let load<'a> = memoize reload<'a>

    let Bus =
        match load<IBusConfiguration>() with
        | Success config -> config
        | Failure _ -> BusConfigurationSection() :> IBusConfiguration

