// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Processor
{
    using System;
    using System.Collections.Generic;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using WindowsAzure.Storage;
    using WindowsAzure.Storage.Blob;

    class AzureStorageCheckpointLeaseManager : ICheckpointManager, ILeaseManager
    {
        EventProcessorHost host;
        TimeSpan leaseDuration;
        TimeSpan leaseRenewInterval;

        static readonly TimeSpan storageMaximumExecutionTime = TimeSpan.FromMinutes(2);
        readonly CloudStorageAccount cloudStorageAccount;
        readonly string leaseContainerName = null;
        readonly string storageBlobPrefix;
        BlobRequestOptions renewRequestOptions;
        OperationContext operationContext = null;
        CloudBlobContainer eventHubContainer;
        CloudBlobDirectory consumerGroupDirectory;

        internal AzureStorageCheckpointLeaseManager(string storageConnectionString, string leaseContainerName, string storageBlobPrefix)
            : this(CloudStorageAccount.Parse(storageConnectionString), leaseContainerName, storageBlobPrefix)
        {
        }

        internal AzureStorageCheckpointLeaseManager(CloudStorageAccount cloudStorageAccount, string leaseContainerName, string storageBlobPrefix)
        {
            if (cloudStorageAccount == null)
            {
                throw new ArgumentNullException(nameof(cloudStorageAccount));
            }

            try
            {
                NameValidator.ValidateContainerName(leaseContainerName);
            }
            catch (ArgumentException)
            {
                throw new ArgumentException(
                    "Azure Storage lease container name is invalid. Please check naming conventions at https://msdn.microsoft.com/en-us/library/azure/dd135715.aspx",
                    nameof(leaseContainerName));
            }

            this.cloudStorageAccount = cloudStorageAccount;
            this.leaseContainerName = leaseContainerName;

            // Convert all-whitespace prefix to empty string. Convert null prefix to empty string.
            // Then the rest of the code only has one case to worry about.
            this.storageBlobPrefix = (storageBlobPrefix != null) ? storageBlobPrefix.Trim() : "";
        }

        // The EventProcessorHost can't pass itself to the AzureStorageCheckpointLeaseManager constructor
        // because it is still being constructed. Do other initialization here also because it might throw and
        // hence we don't want it in the constructor.
        internal void Initialize(EventProcessorHost host)
        {
            this.host = host;

            // Assign partition manager options.
            this.leaseDuration = host.PartitionManagerOptions.LeaseDuration;
            this.leaseRenewInterval = host.PartitionManagerOptions.RenewInterval;

            // Set storage renew request options.
            // Lease renew calls shouldn't wait more than leaseRenewInterval
            this.renewRequestOptions = new BlobRequestOptions()
            {
                ServerTimeout = this.leaseRenewInterval,
                MaximumExecutionTime = TimeSpan.FromMinutes(1)
            };

#if NET461
            // Proxy enabled?
            if (this.host.EventProcessorOptions != null && this.host.EventProcessorOptions.WebProxy != null)
            {
                this.operationContext = new OperationContext()
                {
                    Proxy = this.host.EventProcessorOptions.WebProxy
                };
            }
#endif

            // Create storage client and configure max execution time.
            // Max execution time will apply to any storage calls except renew.
            var storageClient = this.cloudStorageAccount.CreateCloudBlobClient();
            storageClient.DefaultRequestOptions = new BlobRequestOptions()
            {
                MaximumExecutionTime = AzureStorageCheckpointLeaseManager.storageMaximumExecutionTime
            };

            this.eventHubContainer = storageClient.GetContainerReference(this.leaseContainerName);
            this.consumerGroupDirectory = this.eventHubContainer.GetDirectoryReference(this.storageBlobPrefix + this.host.ConsumerGroupName);
        }

        //
        // In this implementation, checkpoints are data that's actually in the lease blob, so checkpoint operations
        // turn into lease operations under the covers.
        //
        public Task<bool> CheckpointStoreExistsAsync()
        {
            return LeaseStoreExistsAsync();
        }

        public Task<bool> CreateCheckpointStoreIfNotExistsAsync()
        {
            return CreateLeaseStoreIfNotExistsAsync();
        }

        public async Task<Checkpoint> GetCheckpointAsync(string partitionId)
        {
    	    AzureBlobLease lease = (AzureBlobLease)await GetLeaseAsync(partitionId).ConfigureAwait(false);
            Checkpoint checkpoint = null;
            if (lease != null && !string.IsNullOrEmpty(lease.Offset))
            {
                checkpoint = new Checkpoint(partitionId)
                {
                    Offset = lease.Offset,
                    SequenceNumber = lease.SequenceNumber
                };
            }

    	    return checkpoint;
        }

        [Obsolete("Use UpdateCheckpointAsync(Lease lease, Checkpoint checkpoint) instead", true)]
        public Task UpdateCheckpointAsync(Checkpoint checkpoint)
        {
            throw new NotImplementedException();
        }

        public async Task<Checkpoint> CreateCheckpointIfNotExistsAsync(string partitionId)
        {
    	    // Normally the lease will already be created, checkpoint store is initialized after lease store.
    	    AzureBlobLease lease = (AzureBlobLease)await CreateLeaseIfNotExistsAsync(partitionId).ConfigureAwait(false);
            Checkpoint checkpoint = new Checkpoint(partitionId, lease.Offset, lease.SequenceNumber);

            return checkpoint;
        }

        public async Task UpdateCheckpointAsync(Lease lease, Checkpoint checkpoint)
        {
            AzureBlobLease newLease = new AzureBlobLease((AzureBlobLease)lease);
            newLease.Offset = checkpoint.Offset;
            newLease.SequenceNumber = checkpoint.SequenceNumber;
            await this.UpdateLeaseAsync(newLease).ConfigureAwait(false);
        }

        public Task DeleteCheckpointAsync(string partitionId)
        {
            // Make this a no-op to avoid deleting leases by accident.
            return Task.FromResult(0);
        }

        //
        // Lease operations.
        //
        public TimeSpan LeaseRenewInterval
        {
            get
            {
                return this.leaseRenewInterval;
            }
        }

        public TimeSpan LeaseDuration
        {
            get
            {
                return this.leaseDuration;
            }
        }

        public Task<bool> LeaseStoreExistsAsync()
        {
            return this.eventHubContainer.ExistsAsync(null, this.operationContext);
        }

        public Task<bool> CreateLeaseStoreIfNotExistsAsync()
        {
            return this.eventHubContainer.CreateIfNotExistsAsync(null, this.operationContext);
        }

        public async Task<bool> DeleteLeaseStoreAsync()
        {
            bool retval = true;

            BlobContinuationToken outerContinuationToken = null;
            do
            {
                BlobResultSegment outerResultSegment = await this.eventHubContainer.ListBlobsSegmentedAsync(outerContinuationToken).ConfigureAwait(false);
                outerContinuationToken = outerResultSegment.ContinuationToken;
                foreach (IListBlobItem blob in outerResultSegment.Results)
                {
                    if (blob is CloudBlobDirectory)
                    {
                        BlobContinuationToken innerContinuationToken = null;
                        do
                        {
                            BlobResultSegment innerResultSegment = await ((CloudBlobDirectory)blob).ListBlobsSegmentedAsync(innerContinuationToken).ConfigureAwait(false);
                            innerContinuationToken = innerResultSegment.ContinuationToken;
                            foreach (IListBlobItem subBlob in innerResultSegment.Results)
                            {
                                try
                                {
                                    await ((CloudBlockBlob)subBlob).DeleteIfExistsAsync().ConfigureAwait(false);
                                }
                                catch (StorageException e)
                                {
                                    ProcessorEventSource.Log.AzureStorageManagerWarning(this.host.HostName, "N/A", "Failure while deleting lease store:", e.ToString());
                                    retval = false;
                                }
                            }
                        }
                        while (innerContinuationToken != null);
                    }
    		        else if (blob is CloudBlockBlob)
                    {
                        try
                        {
                            await ((CloudBlockBlob)blob).DeleteIfExistsAsync().ConfigureAwait(false);
                        }
                        catch (StorageException e)
                        {
                            ProcessorEventSource.Log.AzureStorageManagerWarning(this.host.HostName, "N/A", "Failure while deleting lease store:", e.ToString());
                            retval = false;
                        }
                    }
                }
            }
            while (outerContinuationToken != null);

            return retval;
        }

        public async Task<Lease> GetLeaseAsync(string partitionId) // throws URISyntaxException, IOException, StorageException
        {
    	    AzureBlobLease retval = null;

            CloudBlockBlob leaseBlob = GetBlockBlobReference(partitionId);

            if (await leaseBlob.ExistsAsync(null, this.operationContext).ConfigureAwait(false))
		    {
                retval = await DownloadLeaseAsync(partitionId, leaseBlob).ConfigureAwait(false);
		    }

            return retval;
        }

        public IEnumerable<Task<Lease>> GetAllLeases()
        {
            List<Task<Lease>> leaseFutures = new List<Task<Lease>>();
            IEnumerable<string> partitionIds = this.host.PartitionManager.GetPartitionIdsAsync().Result;
            foreach (string id in partitionIds)
            {
                leaseFutures.Add(GetLeaseAsync(id));
            }

            return leaseFutures;
        }

        public async Task<Lease> CreateLeaseIfNotExistsAsync(string partitionId) // throws URISyntaxException, IOException, StorageException
        {
        	AzureBlobLease returnLease;
    	    try
    	    {
                CloudBlockBlob leaseBlob = GetBlockBlobReference(partitionId);
                returnLease = new AzureBlobLease(partitionId, leaseBlob);
                string jsonLease = JsonConvert.SerializeObject(returnLease);

                ProcessorEventSource.Log.AzureStorageManagerInfo(
                    this.host.HostName,
                    partitionId,
                    "CreateLeaseIfNotExist - leaseContainerName: " + this.leaseContainerName + 
                    " consumerGroupName: " + this.host.ConsumerGroupName + " storageBlobPrefix: " + this.storageBlobPrefix);
                await leaseBlob.UploadTextAsync(
                    jsonLease, 
                    null, 
                    AccessCondition.GenerateIfNoneMatchCondition("*"), 
                    null, 
                    this.operationContext).ConfigureAwait(false);
            }
    	    catch (StorageException se)
    	    {
    		    if (se.RequestInformation.ErrorCode == BlobErrorCodeStrings.BlobAlreadyExists ||
                     se.RequestInformation.ErrorCode == BlobErrorCodeStrings.LeaseIdMissing) // occurs when somebody else already has leased the blob
    		    {
                    // The blob already exists.
                    ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.HostName, partitionId, "Lease already exists");
                    returnLease = (AzureBlobLease)await GetLeaseAsync(partitionId).ConfigureAwait(false);
                }
    		    else
    		    {
                    ProcessorEventSource.Log.AzureStorageManagerError(
                        this.host.HostName,
                        partitionId,
                        "CreateLeaseIfNotExist StorageException - leaseContainerName: " + this.leaseContainerName +
                        " consumerGroupName: " + this.host.ConsumerGroupName + " storageBlobPrefix: " + this.storageBlobPrefix,
                        se.ToString());
    			    throw;
                }
    	    }
    	
    	    return returnLease;
        }

        public Task DeleteLeaseAsync(Lease lease)
        {
            var azureBlobLease = (AzureBlobLease)lease;
            ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.HostName, azureBlobLease.PartitionId, "Deleting lease");
            return azureBlobLease.Blob.DeleteIfExistsAsync();
        }

        public Task<bool> AcquireLeaseAsync(Lease lease)
        {
            return AcquireLeaseCoreAsync((AzureBlobLease)lease);
        }

        async Task<bool> AcquireLeaseCoreAsync(AzureBlobLease lease)
        {
            CloudBlockBlob leaseBlob = lease.Blob;
            bool retval = true;
            string newLeaseId = Guid.NewGuid().ToString();
            string partitionId = lease.PartitionId;
        	try
            {
                string newToken;
                await leaseBlob.FetchAttributesAsync(null, null, this.operationContext).ConfigureAwait(false);
                if (leaseBlob.Properties.LeaseState == LeaseState.Leased)
                {
                    if (string.IsNullOrEmpty(lease.Token))
                    {
                        // We reach here in a race condition: when this instance of EventProcessorHost scanned the
                        // lease blobs, this partition was unowned (token is empty) but between then and now, another
                        // instance of EPH has established a lease (getLeaseState() is LEASED). We normally enforce
                        // that we only steal the lease if it is still owned by the instance which owned it when we
                        // scanned, but we can't do that when we don't know who owns it. The safest thing to do is just
                        // fail the acquisition. If that means that one EPH instance gets more partitions than it should,
                        // rebalancing will take care of that quickly enough.
                        return false;
                    }

                    ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.HostName, lease.PartitionId, "Need to ChangeLease");
                    newToken = await leaseBlob.ChangeLeaseAsync(
                        newLeaseId, 
                        AccessCondition.GenerateLeaseCondition(lease.Token), 
                        null, 
                        this.operationContext).ConfigureAwait(false);
                }
                else
                {
                    ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.HostName, lease.PartitionId, "Need to AcquireLease");

                    try
                    {
                        newToken = await leaseBlob.AcquireLeaseAsync(leaseDuration, newLeaseId, null, null, this.operationContext).ConfigureAwait(false);
                    }
                    catch (StorageException se)
                        when (se.RequestInformation != null
                        && se.RequestInformation.ErrorCode.Equals(BlobErrorCodeStrings.LeaseAlreadyPresent, StringComparison.OrdinalIgnoreCase))
                    {
                        // Either some other host grabbed the lease or checkpoint call renewed it.
                        return false;
                    }
                }

                lease.Token = newToken;
                lease.Owner = this.host.HostName;
                lease.IncrementEpoch(); // Increment epoch each time lease is acquired or stolen by a new host
                await leaseBlob.UploadTextAsync(
                    JsonConvert.SerializeObject(lease),
                    null,
                    AccessCondition.GenerateLeaseCondition(lease.Token),
                    null,
                    this.operationContext).ConfigureAwait(false);
            }
    	    catch (StorageException se)
            {
                throw HandleStorageException(partitionId, se);
            }
    	
    	    return retval;
        }

        public Task<bool> RenewLeaseAsync(Lease lease)
        {
            return RenewLeaseCoreAsync((AzureBlobLease)lease);
        }

        async Task<bool> RenewLeaseCoreAsync(AzureBlobLease lease)
        {
            CloudBlockBlob leaseBlob = lease.Blob;
            string partitionId = lease.PartitionId;

    	    try
            {
                await leaseBlob.RenewLeaseAsync(
                    AccessCondition.GenerateLeaseCondition(lease.Token),
                    this.renewRequestOptions,
                    this.operationContext).ConfigureAwait(false);
            }
    	    catch (StorageException se)
            {
                throw HandleStorageException(partitionId, se);
            }
    	
 	        return true;
        }

        public Task<bool> ReleaseLeaseAsync(Lease lease)
        {
            return ReleaseLeaseCoreAsync((AzureBlobLease)lease);
        }

        async Task<bool> ReleaseLeaseCoreAsync(AzureBlobLease lease)
        {
            ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.HostName, lease.PartitionId, "Releasing lease");

            CloudBlockBlob leaseBlob = lease.Blob;
            string partitionId = lease.PartitionId;

        	try
            {
                string leaseId = lease.Token;
                AzureBlobLease releasedCopy = new AzureBlobLease(lease)
                {
                    Token = string.Empty,
                    Owner = string.Empty
                };
                await leaseBlob.UploadTextAsync(
                    JsonConvert.SerializeObject(releasedCopy), 
                    null, 
                    AccessCondition.GenerateLeaseCondition(leaseId), 
                    null,
                    this.operationContext).ConfigureAwait(false);
                await leaseBlob.ReleaseLeaseAsync(AccessCondition.GenerateLeaseCondition(leaseId)).ConfigureAwait(false);
            }
    	    catch (StorageException se)
            {
                throw HandleStorageException(partitionId, se);
            }
    	
        	return true;
        }

        public Task<bool> UpdateLeaseAsync(Lease lease)
        {
            return UpdateLeaseCoreAsync((AzureBlobLease)lease);
        }

        async Task<bool> UpdateLeaseCoreAsync(AzureBlobLease lease)
        {
    	    if (lease == null)
            {
                return false;
            }

            string partitionId = lease.PartitionId;
            ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.HostName, partitionId, "Updating lease");

            string token = lease.Token;
    	    if (string.IsNullOrEmpty(token))
            {
                return false;
            }

            // First, renew the lease to make sure the update will go through.
            await this.RenewLeaseAsync(lease).ConfigureAwait(false);

            CloudBlockBlob leaseBlob = lease.Blob;
    	    try
            {
                string jsonToUpload = JsonConvert.SerializeObject(lease);
                ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.HostName, lease.PartitionId, $"Raw JSON uploading: {jsonToUpload}");
                await leaseBlob.UploadTextAsync(
                    jsonToUpload, 
                    null, 
                    AccessCondition.GenerateLeaseCondition(token), 
                    null,
                    this.operationContext).ConfigureAwait(false);
            }
    	    catch (StorageException se)
	        {
                throw HandleStorageException(partitionId, se);
	        }
    	
        	return true;
        }

        async Task<AzureBlobLease> DownloadLeaseAsync(string partitionId, CloudBlockBlob blob) // throws StorageException, IOException
        {
            string jsonLease = await blob.DownloadTextAsync().ConfigureAwait(false);

            ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.HostName, partitionId, "Raw JSON downloaded: " + jsonLease);
            AzureBlobLease rehydrated = (AzureBlobLease)JsonConvert.DeserializeObject(jsonLease, typeof(AzureBlobLease));
    	    AzureBlobLease blobLease = new AzureBlobLease(rehydrated, blob);
    	    return blobLease;
        }

        Exception HandleStorageException(string partitionId, StorageException se)
        {
            ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.HostName, partitionId, "HandleStorageException - HttpStatusCode " + se.RequestInformation.HttpStatusCode);
            if (se.RequestInformation.HttpStatusCode == 409 || // conflict
                se.RequestInformation.HttpStatusCode == 412) // precondition failed
            {
                ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.HostName, partitionId,
                    "HandleStorageException - Error code: " + se.RequestInformation.ErrorCode);
                ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.HostName, partitionId,
                    "HandleStorageException - Error message: " + se.RequestInformation.ExtendedErrorInformation?.ErrorMessage);

                if (se.RequestInformation.ErrorCode == null ||
                    se.RequestInformation.ErrorCode == BlobErrorCodeStrings.LeaseLost ||
                    se.RequestInformation.ErrorCode == BlobErrorCodeStrings.LeaseIdMismatchWithLeaseOperation ||
                    se.RequestInformation.ErrorCode == BlobErrorCodeStrings.LeaseIdMismatchWithBlobOperation)
                {
                    return new LeaseLostException(partitionId, se);
                }
            }

            return se;
        }

        CloudBlockBlob GetBlockBlobReference(string partitionId)
        {
            CloudBlockBlob leaseBlob = this.consumerGroupDirectory.GetBlockBlobReference(partitionId);

            // Fixed, keeping workaround commented until full validation.
            // GetBlockBlobReference creates a new ServiceClient thus resets options.
            // Because of this we lose settings like MaximumExecutionTime on the client.
            // Until storage addresses the issue we need to override it here once more.
            // Tracking bug: https://github.com/Azure/azure-storage-net/issues/398
            // leaseBlob.ServiceClient.DefaultRequestOptions = this.storageClient.DefaultRequestOptions;

            return leaseBlob;
        }
    }
}