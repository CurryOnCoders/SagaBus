namespace CurryOn.Msmq

open CurryOn.Common
open CurryOn.Core
open System
open System.Messaging
open System.Threading

type MsmqReader (container: IContainer) =
    let propertyFilter = MessagePropertyFilter(Body = true, SentTime = true, Extension = true)
    let cancellationSource = new CancellationTokenSource()
    let connect queuePath = new MessageQueue(queuePath, MessageReadPropertyFilter = propertyFilter)
    member __.CancellationToken = cancellationSource.Token
    interface ICommandReceiver with
        member __.ReceiveCommands () = 
            tryAsync {
                let queuePaths = Configuration.Current.CommandReceivers 
                                 |> Seq.filter (fun connection -> connection.ConnectionType = typeof<MessageQueue>.Name)
                                 |> Seq.map (fun connection -> connection.ConnectionString)
                                 |> Seq.toList
                let queues = queuePaths |> List.map connect
                return {new IObservable<ICommand> with
                            member __.Subscribe observer =
                                queues 
                                |> List.map (fun queue ->
                                    while cancellationSource.IsCancellationRequested |> not do
                                        let transaction = new MessageQueueTransaction()
                                        transaction.Begin()
                                        let message = queue.Receive(transaction)
                                        let header = message.Extension // Deserialize header to get IMessageMetadata
                                        // TODO: get command from message body based on header information (MessageFormat, MessageName |> Container.getNamedType, etc.)
                                        //observer.OnNext(command)
                                        ()
                                    {new IDisposable with
                                        member __.Dispose () = observer.OnCompleted()
                                    })
                                |> List.reduce (fun d1 d2 -> {new IDisposable with 
                                                                member __.Dispose () =
                                                                    d1.Dispose()
                                                                    d2.Dispose() })
                        
                }
            }
        
