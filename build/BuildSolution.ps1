dotnet restore
    
dotnet build src/Microsoft.Azure.EventHubs/project.json
    
dotnet build src/Microsoft.Azure.EventHubs.Processor/project.json
    
dotnet build test/Microsoft.Azure.EventHubs.UnitTests/project.json
    
dotnet build test/Microsoft.Azure.EventHubs.Processor.UnitTests/project.json