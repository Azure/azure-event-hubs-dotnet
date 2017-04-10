function Build-Solution
{
    Write-Host "Building Event Hubs projects"

    dotnet restore  
    dotnet build src/Microsoft.Azure.EventHubs/project.json
    dotnet build src/Microsoft.Azure.EventHubs.Processor/project.json
    dotnet build test/Microsoft.Azure.EventHubs.UnitTests/project.json
    dotnet build test/Microsoft.Azure.EventHubs.Processor.UnitTests/project.json

    Write-Host "Building complete"
}

function Deploy-AzureResources
{
    if ([bool]$env:ClientSecret `
        -and [bool]$env:TenantId `
        -and [bool]$env:AppId `
        -and [bool]$env:APPVEYOR_BUILD_NUMBER)
    {
        Write-Host "Creating Azure resources"

        $ErrorActionPreference = 'Stop'
        Enable-AzureDataCollection -WarningAction SilentlyContinue | Out-Null
        $BuildVersion = ($env:APPVEYOR_BUILD_NUMBER).Replace(".", "")
    
        $env:ResourceGroupName = "eh-dotnet-av-$BuildVersion-rg"
        $NamespaceName = "eh-dotnet-av-$BuildVersion-ns"
        $StorageAccountName = "ehdotnetav" + $BuildVersion + "sa"
        $Location = 'westus'

        $Password = ConvertTo-SecureString -AsPlainText -Force $env:ClientSecret
        $Credentials = New-Object `
            -TypeName System.Management.Automation.PSCredential `
            -ArgumentList $env:AppId, $Password

        # https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-authenticate-service-principal
        Add-AzureRmAccount -Credential $Credentials -ServicePrincipal -TenantId $env:TenantId | Out-Null
 
        $ResourceGroup = New-AzureRmResourceGroup -Name $env:ResourceGroupName -Location $Location -Force -WarningAction SilentlyContinue
        Write-Host ("Resource group name: " + $ResourceGroup.ResourceGroupName)

	    $EventHubName = 'EventHub1'
        $ArmParameters = @{
            namespaceName = $NamespaceName;
            eventhubName = 'EventHub1';
            consumerGroupName = 'CGroup1';
            storageAccountName = $StorageAccountName;
        }

        $TemplatePath = "$((Get-Location).path)\templates\azuredeploy.json"
    
        $settings = New-AzureRmResourceGroupDeployment `
           -ResourceGroupName $env:ResourceGroupName `
           -TemplateFile $TemplatePath `
           -TemplateParameterObject $ArmParameters `
           -Force `
           -WarningAction SilentlyContinue

        Write-Host "Event Hubs namespace: $NamespaceName"
        Write-Host "Storage account name: $StorageAccountName"

        $env:EVENTHUBCONNECTIONSTRING = $settings.Outputs.Get_Item("namespaceConnectionString").Value + ";EntityPath=$EventHubName"
        $env:EVENTPROCESSORSTORAGECONNECTIONSTRING = $settings.Outputs.Get_Item("storageAccountConnectionString").Value

        Write-Host "Completed creating Azure resources"
    }
    else
    {
        Write-Host "No environment variables present. Skipping Azure deployment."
    }

    # Remove-AzureRmResourceGroup -Name $ResourceGroupName -Force

    # Useful for debugging ARM deployments
    # Get-AzureRmLog -CorrelationId "GUID" -DetailedOutput
}

function Run-UnitTests
{
    if ([bool]$env:EVENTHUBCONNECTIONSTRING -and [bool]$env:EVENTPROCESSORSTORAGECONNECTIONSTRING)
    {
        Write-Host "Running unit tests."

        Invoke-WebRequest -Uri "https://dist.nuget.org/win-x86-commandline/latest/nuget.exe" -OutFile nuget.exe
        & .\nuget.exe install opencover -version 4.6.519
        
        $openCoverConsole = $ENV:APPVEYOR_BUILD_FOLDER + '\OpenCover.4.6.519\tools\OpenCover.Console.exe'
        $target = '-target:C:\Program Files\dotnet\dotnet.exe'
        
        $testProject = $ENV:APPVEYOR_BUILD_FOLDER + '\test\Microsoft.Azure.EventHubs.UnitTests\project.json'
        $targetArgs = '-targetargs: test ' + $testProject + ' -f netcoreapp1.0'
        $filter = '-filter:+[Microsoft.Azure.EventHubs*]* -[Microsoft.Azure.EventHubs.UnitTests]*'
        
        & $openCoverConsole $target $targetArgs $filter '-register:user' '-oldStyle'

        $processorTestProject = $ENV:APPVEYOR_BUILD_FOLDER + '\test\Microsoft.Azure.EventHubs.Processor.UnitTests\project.json'
        $processorTargetArgs = '-targetargs: test ' + $processorTestProject + ' -f netcoreapp1.0'
        $processorFilter = '-filter:+[Microsoft.Azure.EventHubs*]* -[Microsoft.Azure.EventHubs.UnitTests]* -[Microsoft.Azure.EventHubs.Processor.UnitTests]*'
        
        $coverageFile = $ENV:APPVEYOR_BUILD_FOLDER + '\coverage.xml'
        $output = '-output:' + $coverageFile

        & $openCoverConsole $target $processorTargetArgs $processorFilter $output '-mergeoutput' '-register:user' '-oldStyle'

        if (-not $?)
        {
            throw "Unit tests failed."
        }

        $ENV:PATH = 'C:\\Python34;C:\\Python34\\Scripts;' + $ENV:PATH
        python -m pip install --upgrade pip
        pip install git+git://github.com/codecov/codecov-python.git
        codecov -f $coverageFile -t $ENV:CodeCov -X gcov
    }
    else
    {
        Write-Host "No environment variables present. Skipping unit tests."
    }
}

function Delete-AzureResources
{
    if ([bool]$env:ClientSecret -and [bool]$env:AppId)
    {
        Write-Host "Deleting Azure resources"

        $ErrorActionPreference = 'Stop'
    
        $Password = ConvertTo-SecureString -AsPlainText -Force $env:ClientSecret
        $Credentials = New-Object `
            -TypeName System.Management.Automation.PSCredential `
            -ArgumentList $env:AppId, $Password

        Remove-AzureRmResourceGroup -Name $env:ResourceGroupName -WarningAction SilentlyContinue -Force | Out-Null

        Write-Host "Completed deleting Azure resources"
    }
    else
    {
        Write-Host "No environment variables present. Skipping Azure resource deletion"
    }
}