namespace CurryOn.Core

open CurryOn.Common
open System.Configuration
open System.Net

type IConnectionConfiguration =
    abstract member ConnectionType: string
    abstract member ConnectionString: string

type ICommandRouteConfiguration =
    abstract member CommandType: string
    abstract member Destination: string

type ICommandRouterConfiguration =
    abstract member CommandRouterType: string
    abstract member CommandRoutes: ICommandRouteConfiguration seq

type ICredentialConfiguration =
    abstract member UserName: string
    abstract member Password: string

type IEventHubConfiguration =
    abstract member EventStoreIpAddress: IPAddress
    abstract member EventStoreTcpPort: int
    abstract member EventStoreHttpPort: int
    abstract member EventStoreCredentials: ICredentialConfiguration
    abstract member EventStoreEndpoint: IPEndPoint

type IAggregateAgentConfiguration =
    abstract member AggregateName: string<AggregateName>
    abstract member NumberOfAgents: int

type IAgentConfiguration =
    abstract member NumberOfSharedAgents: int
    abstract member AggregateAgents: IAggregateAgentConfiguration seq

type IBusConfiguration =
    inherit IConfiguration
    abstract member CommandReceivers: IConnectionConfiguration seq
    abstract member CommandRouters: ICommandRouterConfiguration seq
    abstract member EventReceivers: IConnectionConfiguration seq
    abstract member EventHub: IEventHubConfiguration
    abstract member Agents: IAgentConfiguration

module Configuration = 
    type ConnectionConfigurationElement () =
        inherit ConfigurationElement()
        [<ConfigurationProperty("type", IsRequired = true)>]
        member this.ConnectionType    
            with get() = this.["type"].ToString()
            and set (value: string) = this.["type"] <- value
        [<ConfigurationProperty("connectionString", IsRequired = true)>]
        member this.ConnectionString    
            with get() = this.["connectionString"].ToString()
            and set (value: string) = this.["connectionString"] <- value
        interface IConnectionConfiguration with
            member this.ConnectionType = this.ConnectionType
            member this.ConnectionString = this.ConnectionString

    [<ConfigurationCollection(typeof<ConnectionConfigurationElement>, AddItemName = "connection")>]
    type ConnectionConfigurationElementCollection() =
        inherit ConfigurationElementCollection()
        override this.CreateNewElement() = new ConnectionConfigurationElement() :> ConfigurationElement
        override this.GetElementKey element = (element |> unbox<ConnectionConfigurationElement>).ConnectionType |> box
        interface IConnectionConfiguration seq with
            member this.GetEnumerator() = (Seq.cast<IConnectionConfiguration> this).GetEnumerator()

    type CommandRouteConfigurationElement () =
        inherit ConfigurationElement()
        [<ConfigurationProperty("type", IsRequired = true)>]
        member this.CommandType    
            with get() = this.["type"].ToString()
            and set (value: string) = this.["type"] <- value
        [<ConfigurationProperty("destination", IsRequired = true)>]
        member this.Destination    
            with get() = this.["destination"].ToString()
            and set (value: string) = this.["destination"] <- value
        interface ICommandRouteConfiguration with
            member this.CommandType = this.CommandType
            member this.Destination = this.Destination

    [<ConfigurationCollection(typeof<CommandRouteConfigurationElement>, AddItemName = "commandRoute")>]
    type CommandRouteConfigurationElementCollection() =
        inherit ConfigurationElementCollection()
        override this.CreateNewElement() = new CommandRouteConfigurationElement() :> ConfigurationElement
        override this.GetElementKey element = (element |> unbox<CommandRouteConfigurationElement>).CommandType |> box
        interface ICommandRouteConfiguration seq with
            member this.GetEnumerator() = (Seq.cast<ICommandRouteConfiguration> this).GetEnumerator()

    type CommandRouterConfigurationElement () =
        inherit ConfigurationElement()
        [<ConfigurationProperty("type", IsRequired = true)>]
        member this.CommandRouterType    
            with get() = this.["type"].ToString()
            and set (value: string) = this.["type"] <- value
        [<ConfigurationProperty("commandRoutes")>]
        member this.CommandRoutes
            with get () = this.["commandRoutes"] :?> CommandRouteConfigurationElementCollection
            and set (value: CommandRouteConfigurationElementCollection) = this.["commandRoutes"] <- value
        interface ICommandRouterConfiguration with
            member this.CommandRouterType = this.CommandRouterType
            member this.CommandRoutes = this.CommandRoutes :> ICommandRouteConfiguration seq

    [<ConfigurationCollection(typeof<CommandRouteConfigurationElement>, AddItemName = "commandRouter")>]
    type CommandRouterConfigurationElementCollection() =
        inherit ConfigurationElementCollection()
        override this.CreateNewElement() = new CommandRouterConfigurationElement() :> ConfigurationElement
        override this.GetElementKey element = (element |> unbox<CommandRouterConfigurationElement>).CommandRouterType |> box
        interface ICommandRouterConfiguration seq with
            member this.GetEnumerator() = (Seq.cast<ICommandRouterConfiguration> this).GetEnumerator()

    type CredentialConfigurationElement () =
        inherit ConfigurationElement ()
        [<ConfigurationProperty("userName", IsRequired = true)>]
        member this.UserName    
            with get() = this.["userName"].ToString()
            and set (value: string) = this.["userName"] <- value
        [<ConfigurationProperty("password")>]
        member this.Password
            with get () = this.["password"].ToString()
            and set (value: string) = this.["password"] <- value
        interface ICredentialConfiguration with
            member this.UserName = this.UserName
            member this.Password = this.Password

    type EventHubConfigurationElement () =
        inherit ConfigurationElement()
        [<ConfigurationProperty("ipAddress", IsRequired = false, DefaultValue = "127.0.0.1")>]
        member this.IpAddress    
            with get() = this.["ipAddress"].ToString()
            and set (value: string) = this.["ipAddress"] <- value
        [<ConfigurationProperty("tcpPort", IsRequired = false, DefaultValue = 1113)>]
        member this.TcpPort    
            with get() = this.["tcpPort"] :?> int
            and set (value: int) = this.["tcpPort"] <- value
        [<ConfigurationProperty("httpPort", IsRequired = false, DefaultValue = 2113)>]
        member this.HttpPort    
            with get() = this.["httpPort"] :?> int
            and set (value: int) = this.["httpPort"] <- value
        [<ConfigurationProperty("credentials")>]
        member this.Credentials
            with get() = this.["credentials"] :?> CredentialConfigurationElement
            and set (value: CredentialConfigurationElement) = this.["credentials"] <- value
        member private this.EventStoreIpAddress = IPAddress.Parse(this.IpAddress)
        interface IEventHubConfiguration with
            member this.EventStoreIpAddress = this.EventStoreIpAddress
            member this.EventStoreTcpPort = this.TcpPort
            member this.EventStoreHttpPort = this.HttpPort
            member this.EventStoreCredentials = this.Credentials :> ICredentialConfiguration
            member this.EventStoreEndpoint = IPEndPoint(this.EventStoreIpAddress, this.TcpPort)

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
        inherit Configuration.ContextConfigurationSection()
        [<ConfigurationProperty("commandReceivers")>]
        member this.CommandReceivers
            with get () = this.["commandReceivers"] :?> ConnectionConfigurationElementCollection
            and set (value: ConnectionConfigurationElementCollection) = this.["commandReceivers"] <- value
        [<ConfigurationProperty("commandRouters")>]
        member this.CommandRouters
            with get () = this.["commandRouters"] :?> CommandRouterConfigurationElementCollection
            and set (value: CommandRouterConfigurationElementCollection) = this.["commandRouters"] <- value
        [<ConfigurationProperty("eventReceivers")>]
        member this.EventReceivers
            with get () = this.["eventReceivers"] :?> ConnectionConfigurationElementCollection
            and set (value: ConnectionConfigurationElementCollection) = this.["eventReceivers"] <- value
        [<ConfigurationProperty("eventHub")>]
        member this.EventHub
            with get() = this.["eventHub"] :?> EventHubConfigurationElement
            and set (value: EventHubConfigurationElement) = this.["eventHub"] <- value
        [<ConfigurationProperty("agents")>]
        member this.Agents 
            with get () = this.["agents"] :?> AgentConfigurationElement
            and set (value: AgentConfigurationElement) = this.["agents"] <- value
        interface IBusConfiguration with
            member this.CommandReceivers = this.CommandReceivers :> IConnectionConfiguration seq
            member this.CommandRouters = this.CommandRouters :> ICommandRouterConfiguration seq
            member this.EventReceivers = this.EventReceivers :> IConnectionConfiguration seq
            member this.EventHub = this.EventHub :> IEventHubConfiguration
            member this.Agents = this.Agents :> IAgentConfiguration
            
    let Current = 
        match Configuration.load<IBusConfiguration>() with
        | Success config -> config
        | Failure _ -> new BusConfigurationSection() :> IBusConfiguration