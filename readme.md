<p align="center">
  <img src="event-hubs.png" alt="Microsoft Azure Event Hubs" width="100"/>
</p>

# Microsoft Azure Event Hubs Client for .NET

|Build/Package|Status|
|------|-------------|
|master|[![Build status](https://ci.appveyor.com/api/projects/status/p71xb6o7233m7gi3/branch/master?svg=true)](https://ci.appveyor.com/project/jtaubensee/azure-event-hubs-dotnet/branch/master)|
|dev|[![Build status](https://ci.appveyor.com/api/projects/status/p71xb6o7233m7gi3/branch/master?svg=true)](https://ci.appveyor.com/project/jtaubensee/azure-event-hubs-dotnet/branch/dev)|
|Microsoft.Azure.EventHubs|[![NuGet Version and Downloads count](https://buildstats.info/nuget/Microsoft.Azure.EventHubs?includePreReleases=true)](https://www.nuget.org/packages/Microsoft.Azure.EventHubs/)|
|Microsoft.Azure.EventHubs.Processor|[![NuGet Version and Downloads count](https://buildstats.info/nuget/Microsoft.Azure.EventHubs.Processor?includePreReleases=true)](https://www.nuget.org/packages/Microsoft.Azure.EventHubs.Processor/)|

This library is built using .NET Standard 1.3. For more information on what platforms are supported see [.NET Platforms Support](https://docs.microsoft.com/en-us/dotnet/articles/standard/library#net-platforms-support).

Azure Event Hubs is a highly scalable publish-subscribe service that can ingest millions of events per second and stream them into multiple applications. This lets you process and analyze the massive amounts of data produced by your connected devices and applications. Once Event Hubs has collected the data, you can retrieve, transform and store it by using any real-time analytics provider or with batching/storage adapters. 

Refer to the [online documentation](https://azure.microsoft.com/services/event-hubs/) to learn more about Event Hubs in general.

## How to provide feedback

See our [Contribution Guidelines](./.github/CONTRIBUTING.md).

## Overview

The .NET client library for Azure Event Hubs allows for both sending events to and receiving events from an Azure Event Hub. 

An **event publisher** is a source of telemetry data, diagnostics information, usage logs, or other log data, as 
part of an emvbedded device solution, a mobile device application, a game title running on a console or other device, 
some client or server based business solution, or a web site.  

An **event consumer** picks up such information from the Event Hub and processes it. Processing may involve aggregation, complex 
computation and filtering. Processing may also involve distribution or storage of the information in a raw or transformed fashion.
Event Hub consumers are often robust and high-scale platform infrastructure parts with built-in analytics capabilites, like Azure 
Stream Analytics, Apache Spark, or Apache Storm.   
   
Most applications will act either as an event publisher or an event consumer, but rarely both. The exception are event 
consumers that filter and/or transform event streams and then forward them on to another Event Hub; an example for such is Azure Stream Analytics.

### Getting Started

To get started sending events to an Event Hub refer to [Get started sending messages to Event Hubs in .NET Core](https://github.com/Azure/azure-event-hubs/tree/master/samples/SampleSender).

To get started receiving events with the **EventProcessorHost** refer to [Get started receiving messages with the EventProcessorHost in .NET Core](https://github.com/Azure/azure-event-hubs/tree/master/samples/SampleEphReceiver).  

### Running the unit tests 

In order to run the unit tests, you will need to do the following:

1. Create an Event Hub

2. Create a consumer group on that Event Hub called `cgroup1` in addition to the default consumer group.

3. Create a storage account

4. Add the following Environment Variables with the corresponding connection strings:

  1. `EVENTHUBCONNECTIONSTRING` - *The EntityPath is required in this string.*

  2. `EVENTPROCESSORSTORAGECONNECTIONSTRING`

<a href="https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FAzure%2Fazure-event-hubs-dotnet%2Fmaster%2Ftemplates%2Fazuredeploy.json" target="_blank">
    <img src="http://azuredeploy.net/deploybutton.png"/>
</a>
