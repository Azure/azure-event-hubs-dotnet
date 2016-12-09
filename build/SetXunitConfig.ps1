$xUnitConfig = "
{
    `"parallelizeAssembly`": false,
    `"parallelizeTestCollections`": false
}"

New-Item test/Microsoft.Azure.EventHubs.UnitTests/bin/debug/netcoreapp1.0/xunit.runner.json -type file -force -value $xUnitConfig
    
New-Item test/Microsoft.Azure.EventHubs.UnitTests/bin/Debug/net46/win7-x64/xunit.runner.json -type file -force -value $xUnitConfig

New-Item test/Microsoft.Azure.EventHubs.Processor.UnitTests/bin/debug/netcoreapp1.0/xunit.runner.json -type file -force -value $xUnitConfig

New-Item test/Microsoft.Azure.EventHubs.Processor.UnitTests/bin/Debug/net46/win7-x64/xunit.runner.json -type file -force -value $xUnitConfig