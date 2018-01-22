# CurryOn
An initiative for building frameworks and packages for F# to enable development of modern applications using the principles of Domain-Driven Design.  Includes projects to facilitate Event Sourcing, CQRS, and Railway-Oriented Programming, as well as libraries of common tools to simplify Enterprise development in F#.

## Projects

### Curryon.Akka.Persistence.Elasticsearch
An Akka.net Persistence package for [Elasticsearch](http://elastic.co).  Supports the use of Elasticsearch Indices as Akka.net Event Journals and Snapshot Stores, as well as providing an AsyncReadJournal based on the Elasticsearch Search API.  Uses the CurryOn.Elastic.FSharp library to interface with Elasticsearch.

### Curryon.Akka.Persistence.EventStore
An Akka.net Persistence package for [EventStore](http://geteventstore.com).  Supports the use of EventStore Event Streams as Akka.net Event Journals, where each PersistenceId in Akka.net is a unique stream in EventStore.  Also supports using EventStore as an Akka.net SnapshotStore, creating Snapshot and SnapshotMetadata streams for each Akka.net PersistenceId.  Implements the standard Akka.net Persistence Query API using EventStore Subscriptions.

### CurryOn.Common
A library of useful tools for F# development

### CurryOn.DependencyInjection
A pure-F# Depdendency Injection framework, providing an IoC Container and a Type Injector.

### CurryOn.FSharp.Control 
An extension of the FSharp.Control namespace providing a framework for enabling the use of [Railway-Oriented Programming](https://fsharpforfunandprofit.com/rop/) patterns in F#.  Encapsulates functionality from the [Task Parallel Library (TPL)](https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/task-parallel-library-tpl), [Async Workflows](https://docs.microsoft.com/en-us/dotnet/fsharp/language-reference/asynchronous-workflows), and [Lazy Computations](https://docs.microsoft.com/en-us/dotnet/fsharp/language-reference/lazy-computations) as **Operations**, and provides a Computation Expression Builder and a library of functions and operators for working with Operations and their Results.  

### CurryOn.Elastic.FSharp
An F#-idiomatch Library for [Elasticsearch](http://elastic.co), including a representation of the Elasticsearch Query Model as Discriminated Unions, and F# modules for indexing documents as well as creating and maintaining indexes.


### CurryOn.SagaBus 
A platform built in F# for creating modern, highly scalable Enterprise applications based on Domain-Driven Design and the CQRS architectural pattern, centered around a Bus for hosting Sagas (business processes) to handle associated Commands and Events.  This project is divided into sub-projects to build the modular functionality, and leverages the projects above for standard features.

#### CurryOn.Core
The core functionality of the SagaBus, including an basic Domain Modeling types such as IAggregate and basic message types such as ICommand and IEvent, as well as common features such as Configuration and Serialization.

#### CurryOn.EventStore
An F# idiomatic library for using the [EventStore](http://geteventstore.com) .NET Client API and the Atom HTTP API.  EventStore can be used to implement the IEventHub and IEventReceiver interfaces of the CurryOn SagaBus.

#### CurryOn.EventStreamDb
A very early work-in-progress of a pure F#, from-scratch functional Event Stream database.  An append-only data store for persisting domain events in streams based on the Aggregate Key.  This project will also ultimately be used to implement the IEventHub and IEventReceiver interfaces.

#### CurryOn.Msmq
An implementation of the CurryOn SagaBus ICommandRouter and ICommandReceiver interfaces using Microsoft MSMQ as the command transport layer.

