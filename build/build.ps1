$ErrorActionPreference = 'Stop'

$isAppveyor = [bool]$env:APPVEYOR
$configuration = if ($CONFIGURATION -ne $null) { $CONFIGURATION  } else { 'Debug' }
$platform = if ($PLATFORM -ne $null) { $PLATFORM } else { 'Any CPU' }
$projectFolder = if ($ENV:APPVEYOR_BUILD_FOLDER -ne $null) { "$ENV:APPVEYOR_BUILD_FOLDER" } else { $(Get-Location).path }
$buildFolder = $projectFolder + '\build\'
$runtime = if ($ENV:DotNetRunTime -ne $null) { $ENV:DotNetRunTime } else { 'netcoreapp1.0' }
$artifactsFolder = $buildFolder + 'artifacts\'
$appProject = $projectFolder + '\src\Microsoft.Azure.EventHubs\Microsoft.Azure.EventHubs.csproj'
$testProject = $projectFolder + '\test\Microsoft.Azure.EventHubs.Tests\Microsoft.Azure.EventHubs.Tests.csproj'
$processorAppProject = $projectFolder + '\src\Microsoft.Azure.EventHubs.Processor\Microsoft.Azure.EventHubs.Processor.csproj'
$coverageFile = $buildFolder + 'coverage.xml'
$appNamespace = 'Microsoft.Azure.EventHubs'
$testNamespace = 'Microsoft.Azure.EventHubs.Tests'

# Environment variables
$connectionStringVariableName = 'azure-event-hubs-dotnet/connectionstring'
$storageConnectionStringVariableName = 'azure-event-hubs-dotnet/storageconnectionstring'
$codeCovSecret = [Environment]::GetEnvironmentVariable('azure-event-hubs-dotnet/CodeCovSecret')
$clientSecret = [Environment]::GetEnvironmentVariable('azure-event-hubs-dotnet/ClientSecret')
$tenantId = [Environment]::GetEnvironmentVariable('azure-event-hubs-dotnet/TenantId')
$appId = [Environment]::GetEnvironmentVariable('azure-event-hubs-dotnet/AppId')
$canDeploy = ([bool]$clientSecret -and [bool]$tenantId -and [bool]$appId)
$skipCodeCoverage = if ([bool][Environment]::GetEnvironmentVariable('azure-event-hubs-dotnet/SkipCodeCoverage')) { $true } else { $false }

function Build-Solution
{
    Write-Host "Building projects"

    # Restore solution files
    MSBuild.exe Microsoft.Azure.EventHubs.sln /t:restore /p:Configuration=$configuration /p:Platform=$platform /verbosity:minimal

    # $? Returns True or False value indicating whether previous command ended with an error.
    # This is used to throw an error that will cause the AppVeyor process to fail as expected.
    if (-not $?)
    {
        throw "Package restore failed."
    }

    # Build solution
    MSBuild.exe Microsoft.Azure.EventHubs.sln /p:Configuration=$configuration /p:Platform=$platform /verbosity:minimal

    if (-not $?)
    {
        throw "Build failed."
    }
    else
    {
        Write-Host "Building complete."   
    }
}

function Deploy-AzureResources
{
    $resourceNamePrefix = 'eh-dotnet-av-'
    Write-Host "Creating Azure resources"

    Enable-AzureDataCollection -WarningAction SilentlyContinue | Out-Null
    $buildVersion =
    if ($isAppveyor)
    {
        ($env:APPVEYOR_BUILD_NUMBER + $env:APPVEYOR_JOB_NUMBER).Replace(".", "")
    }
    else { Get-Random }

    $global:resourceGroupName = $resourceNamePrefix + $buildVersion + '-rg'
    $namespaceName = $resourceNamePrefix + $buildVersion + '-ns'
    $eventHubName = 'EventHub1'
    $storageAccountName = ($resourceNamePrefix + $buildVersion).Replace("-", "")
    
    $location =
    if ([bool][Environment]::GetEnvironmentVariable('azure-event-hubs-dotnet/location'))
    {
        [Environment]::GetEnvironmentVariable('azure-event-hubs-dotnet/location')
    } else { 'westus' }

    $password = ConvertTo-SecureString -AsPlainText -Force $clientSecret
    $credentials = New-Object `
        -TypeName System.Management.Automation.PSCredential `
        -ArgumentList $appId, $password

    # https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-authenticate-service-principal
    Add-AzureRmAccount -Credential $credentials -ServicePrincipal -TenantId $tenantId | Out-Null

    $resourceGroup = New-AzureRmResourceGroup -Name $resourceGroupName -Location $location -Force -WarningAction SilentlyContinue
    Write-Host ("Resource group name: " + $resourceGroup.ResourceGroupName)

    $armParameters = @{
        namespaceName = $namespaceName;
        eventhubName = $eventHubName;
        consumerGroupName = 'CGroup1';
        storageAccountName = $storageAccountName;
    }

    $templatePath = $buildFolder + 'azuredeploy.json'

    $settings = New-AzureRmResourceGroupDeployment `
        -ResourceGroupName $resourceGroupName `
        -TemplateFile $templatePath `
        -TemplateParameterObject $armParameters `
        -Force `
        -WarningAction SilentlyContinue

    Write-Host "Namespace: $namespaceName"
    Write-Host "Storage account name: $storageAccountName"
    
    $namespaceConnectionString = $settings.Outputs.Get_Item("namespaceConnectionString").Value + ";EntityPath=$eventHubName"
    [Environment]::SetEnvironmentVariable($connectionStringVariableName, $namespaceConnectionString)

    $storageAccountConnectionString = $settings.Outputs.Get_Item("storageAccountConnectionString").Value
    [Environment]::SetEnvironmentVariable($storageConnectionStringVariableName, $storageAccountConnectionString)

    Write-Host "Completed creating Azure resources"

    # Useful for debugging ARM deployments
    # Get-AzureRmLog -CorrelationId "GUID" -DetailedOutput
}

function Run-UnitTests
{
    if ($skipCodeCoverage)
    {
        dotnet test $testProject -f $runtime
        if (-not $?)
        {
            throw "Unit tests failed."
        }
        return;
    }
    
    Write-Host "Running unit tests."
    
    if (-Not (Test-Path .\nuget.exe))
    {
        Invoke-WebRequest -Uri "https://dist.nuget.org/win-x86-commandline/latest/nuget.exe" -outfile $buildFolder\nuget.exe
    }

    $openCoverVersion = '4.6.712'
    $openCoverNuPkgOutFile = $buildFolder + 'OpenCover.' + $openCoverVersion + '.nupkg'

    # Using a temporary version of OpenCover until a NuGet is published. https://github.com/OpenCover/opencover/issues/669
    # Once there is a new NuGet package, this if statement can be removed, and so can '-source $buildFolder' in the line after
    if (-Not (Test-Path $openCoverNuPkgOutFile)) {
        Invoke-WebRequest `
            -Uri "https://ci.appveyor.com/api/buildjobs/upad53qdyo1iv382/artifacts/main%2Fbin%2Fpackages%2Fnuget%2Fopencover%2FOpenCover.4.6.712.nupkg" `
            -OutFile $openCoverNuPkgOutFile
    }

    & $buildFolder\nuget.exe install OpenCover -version $openCoverVersion -SolutionDirectory $buildFolder -source $buildFolder

    $openCoverConsole = $buildFolder + 'packages\' + 'OpenCover.' + $openCoverVersion + '\tools\OpenCover.Console.exe'
    $target = '-target:C:\Program Files\dotnet\dotnet.exe'
    $targetArgs = '-targetargs: test ' + $testProject + ' -f ' + $runtime
    $filter = '-filter:+[' + $appNamespace + '*]* -[' + $testNamespace + ']*'
    $output = '-output:' + $coverageFile

    & $openCoverConsole $target $targetArgs $filter $output '-register:user' '-oldStyle'

    if (-not $?)
    {
        throw "Unit tests failed."
    }

    if (-Not (Test-Path $coverageFile))
    {
        return
    }
    if ([bool]$codeCovSecret)
    {
        $ENV:PATH = 'C:\\Python34;C:\\Python34\\Scripts;' + $ENV:PATH
        python -m pip install --upgrade pip
        pip install git+git://github.com/codecov/codecov-python.git
        codecov -f $coverageFile -t $codeCovSecret -X gcov   
    }
    else
    {
        $reportGeneratorVersion = '2.5.7'
        & $buildFolder\nuget.exe install ReportGenerator -version $reportGeneratorVersion -SolutionDirectory $buildFolder
        $reportGenerator = $buildFolder + 'packages\' + 'ReportGenerator.' + $reportGeneratorVersion + '\tools\ReportGenerator.exe'
        $targetDirectory = $buildFolder + 'OpenCoverReport\'
        & $reportGenerator -reports:$coverageFile -targetdir:$targetDirectory
    }
}

function CopyArtifacts
{
    if (-Not $isAppveyor)
    {
        return
    }
    New-Item -ItemType Directory -Force -Path $artifactsFolder | Out-Null
    MSBuild.exe $appProject /t:pack /p:Configuration=$configuration /p:Platform=$platform /p:PackageOutputPath=$artifactsFolder /verbosity:minimal
    MSBuild.exe $processorAppProject /t:pack /p:Configuration=$configuration /p:Platform=$platform /p:PackageOutputPath=$artifactsFolder /verbosity:minimal
    if (Test-Path $coverageFile)
    {
        Copy-Item $coverageFile $artifactsFolder
    }
}

function Delete-AzureResources
{
    Write-Host "Deleting Azure resources"

    $password = ConvertTo-SecureString -AsPlainText -Force $clientSecret
    $credentials = New-Object `
        -TypeName System.Management.Automation.PSCredential `
        -ArgumentList $appId, $password

    Remove-AzureRmResourceGroup -Name $resourceGroupName -WarningAction SilentlyContinue -Force | Out-Null

    Write-Host "Completed deleting Azure resources"
}

Build-Solution
if (-Not $canDeploy -and -Not [bool][Environment]::GetEnvironmentVariable($connectionStringVariableName)) {
    return
}
try {
    if ($canDeploy -and -not [bool][Environment]::GetEnvironmentVariable($connectionStringVariableName)) {
        Deploy-AzureResources
    }
    if ([bool][Environment]::GetEnvironmentVariable($connectionStringVariableName)) {
        Run-UnitTests
        CopyArtifacts
    }    
}
catch {
	throw
}
finally {
    if ($canDeploy -and $resourceGroupName) {
        Delete-AzureResources
    }
}
