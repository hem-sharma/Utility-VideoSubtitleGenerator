$localTargetDirectory = "@targetDirectory"
$BlobName = "@blobName"
Get-AzureStorageBlobContent -Blob $BlobName -Container $ContainerName ` 
        -Destination $localTargetDirectory -Context $ctx

$BlobName = "gizmodo_groundhog_texting.jpg" 
Get-AzureStorageBlobContent -Blob $BlobName -Container $ContainerName ` 
        -Destination $localTargetDirectory -Context $ctx

$BlobName = "GuyEyeingOreos.png" 
Get-AzureStorageBlobContent -Blob $BlobName -Container $ContainerName ` 
        -Destination $localTargetDirectory -Context $ctx