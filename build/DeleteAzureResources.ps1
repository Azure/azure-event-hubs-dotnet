if ([bool]$env:ClientSecret `
    -and [bool]$env:TenantId `
    -and [bool]$env:AppId)
{
    $ErrorActionPreference = 'Stop'
    
    $Password = ConvertTo-SecureString -AsPlainText -Force $env:ClientSecret
    $Credentials = New-Object `
        -TypeName System.Management.Automation.PSCredential `
        -ArgumentList $env:AppId, $Password

    Remove-AzureRmResourceGroup -Name $env:ResourceGroupName -Force
}