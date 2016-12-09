if ([bool]$env:EVENTHUBCONNECTIONSTRING -and [bool]$env:EVENTPROCESSORSTORAGECONNECTIONSTRING)
{
    Write-Host "Running unit tests."

    dotnet test test/Microsoft.Azure.EventHubs.UnitTests/project.json
    
    dotnet test test/Microsoft.Azure.EventHubs.Processor.UnitTests/project.json
}
else
{
    Write-Host "No environment variables present. Skipping unit tests."
}