namespace CurryOn.Msmq

open CurryOn.Common
open CurryOn.Core
open System
open System.Messaging
open System.Threading
open System.Transactions;

type MsmqReader (container: IContainer) =
    let propertyFilter = MessagePropertyFilter(Body = true, CorrelationId = true, Extension = true, Id = true, Label = true, SentTime = true)
    let configuration = Configuration.Bus.CommandReceivers |> Seq.find (fun receiver -> receiver.ReceiverType = typeof<MsmqReader>.Name)
    let cancellationSource = new CancellationTokenSource()
    let transactionOptions = TransactionOptions(IsolationLevel = IsolationLevel.ReadCommitted, Timeout = configuration.ReceiveTimeout)
    let connect queuePath = new MessageQueue(queuePath, MessageReadPropertyFilter = propertyFilter)
    member __.CancellationToken = cancellationSource.Token
    interface ICommandReceiver with
        member __.ReceiveCommands () = 
            task {
                let queuePaths = 
                    configuration.Connections
                    |> Seq.map (fun connection -> connection.ConnectionString)
                    |> Seq.toList
                let queues = queuePaths |> List.map connect
                return {new IObservable<ICommand> with
                            member __.Subscribe observer =
                                queues 
                                |> List.map (fun queue -> async {
                                    while cancellationSource.IsCancellationRequested |> not do
                                        let transaction = new TransactionScope(TransactionScopeOption.RequiresNew, transactionOptions, TransactionScopeAsyncFlowOption.Enabled)
                                        let message = queue.Receive(MessageQueueTransactionType.Automatic)
                                        let header = message.Extension // Deserialize header to get IMessageMetadata
                                        // TODO: get command from message body based on header information (MessageFormat, MessageName |> Container.getNamedType, etc.)
                                        //observer.OnNext(command)
                                        ()
                                    return {new IDisposable with
                                        member __.Dispose () = observer.OnCompleted()
                                    }})
                                |> Async.Parallel
                                |> Async.RunSynchronously
                                |> Array.reduce (fun d1 d2 -> {new IDisposable with 
                                                                member __.Dispose () =
                                                                    d1.Dispose()
                                                                    d2.Dispose() })
                        
                }
            }
        
