if ([bool]$env:ClientSecret `
    -and [bool]$env:TenantId `
    -and [bool]$env:AppId `
    -and [bool]$env:APPVEYOR_BUILD_NUMBER)
{
    $ErrorActionPreference = 'Stop'
    Enable-AzureDataCollection
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
    Add-AzureRmAccount -Credential $Credentials -ServicePrincipal -TenantId $env:TenantId
 
    New-AzureRmResourceGroup -Name $env:ResourceGroupName -Location $Location -Force

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
       -Force

    $env:EVENTHUBCONNECTIONSTRING = $settings.Outputs.Get_Item("namespaceConnectionString").Value + ";EntityPath=$EventHubName"
    $env:EVENTPROCESSORSTORAGECONNECTIONSTRING = $settings.Outputs.Get_Item("storageAccountConnectionString").Value
}
else
{
    Write-Host "Environment Variables not found."
}

# Remove-AzureRmResourceGroup -Name $ResourceGroupName -Force

# Useful for debugging ARM deployments
# Get-AzureRmLog -CorrelationId "GUID" -DetailedOutput