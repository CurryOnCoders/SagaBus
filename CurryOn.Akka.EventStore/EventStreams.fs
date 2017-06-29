namespace CurryOn.Akka.EventStore

open Akka.Streams.Dsl
open EventStore.ClientAPI
open CurryOn.Common
open Reactive.Streams
open System

module Streams =
    type SystemStreams =
        static member All = "$all"
        static member Streams = "$streams"

    let private defaultSettings = CatchUpSubscriptionSettings.Default

    let private createSource<'source> (eventStore: IEventStoreConnection) streamId start batchSize resolveLinks mapEvent =
        let settings = CatchUpSubscriptionSettings(defaultSettings.MaxLiveQueueSize, batchSize, false, resolveLinks)
        {new IPublisher<'source> with
            member __.Subscribe subscriber =
                let notify (event: ResolvedEvent) =
                    if event.Event |> isNotNull
                    then subscriber.OnNext(event.Event |> mapEvent)
                eventStore.SubscribeToStreamFrom(streamId, start |> Nullable, settings, (fun _ event -> notify event)) 
                |> ignore
        } |> Source.FromPublisher

    let getAllStreams eventStore =
        createSource eventStore SystemStreams.Streams 0L defaultSettings.ReadBatchSize defaultSettings.ResolveLinkTos 
        <| fun event -> event.EventStreamId
        
    let getAllEvents eventStore =
        createSource eventStore SystemStreams.All 0L defaultSettings.ReadBatchSize defaultSettings.ResolveLinkTos 
        <| fun event -> event

    let getStreamEvents eventStore streamId =
        createSource eventStore streamId 0L defaultSettings.ReadBatchSize defaultSettings.ResolveLinkTos
        <| fun event -> event