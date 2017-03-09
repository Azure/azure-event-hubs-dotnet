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
    using WindowsAzure.Storage.Blob.Protocol;

    sealed class AzureStorageCheckpointLeaseManager : ICheckpointManager, ILeaseManager
    {
        EventProcessorHost host;
        readonly string storageConnectionString;
        string leaseContainerName = null;
        string storageBlobPrefix;

        CloudBlobClient storageClient;
        CloudBlobContainer eventHubContainer;
        CloudBlobDirectory consumerGroupDirectory;

        static readonly TimeSpan storageMaximumExecutionTime = TimeSpan.FromMinutes(2);
        static readonly TimeSpan leaseDuration = TimeSpan.FromSeconds(30);
        static readonly TimeSpan leaseRenewInterval = TimeSpan.FromSeconds(10);

        // Lease renew calls shouldn't wait more than leaseRenewInterval
        readonly BlobRequestOptions renewRequestOptions = new BlobRequestOptions()
        {
            ServerTimeout = leaseRenewInterval,
            MaximumExecutionTime = TimeSpan.FromMinutes(1)
        };

        internal AzureStorageCheckpointLeaseManager(string storageConnectionString, string leaseContainerName, string storageBlobPrefix)
        {
            if (string.IsNullOrEmpty(storageConnectionString))
            {
                throw new ArgumentNullException(nameof(storageConnectionString));
            }

            // Validate lease container name.
            if (!Regex.IsMatch(leaseContainerName, @"^[a-z0-9](([a-z0-9\-[^\-])){1,61}[a-z0-9]$"))
            {
                throw new ArgumentException(
                    "Azure Storage lease container name is invalid. Please check naming conventions at https://msdn.microsoft.com/en-us/library/azure/dd135715.aspx",
                   nameof(leaseContainerName));
            }

            this.storageConnectionString = storageConnectionString;
            this.leaseContainerName = leaseContainerName;

            // Convert all-whitespace prefix to empty string. Convert null prefix to empty string.
            // Then the rest of the code only has one case to worry about.
            this.storageBlobPrefix = (storageBlobPrefix != null) ? storageBlobPrefix.Trim() : "";
        }

        // The EventProcessorHost can't pass itself to the AzureStorageCheckpointLeaseManager constructor
        // because it is still being constructed. Do other initialization here also because it might throw and
        // hence we don't want it in the constructor.
        internal void Initialize(EventProcessorHost host) // throws InvalidKeyException, URISyntaxException, StorageException
        {
            this.host = host;
            this.storageClient = CloudStorageAccount.Parse(this.storageConnectionString).CreateCloudBlobClient();
            BlobRequestOptions options = new BlobRequestOptions();
            options.MaximumExecutionTime = AzureStorageCheckpointLeaseManager.storageMaximumExecutionTime;
            this.storageClient.DefaultRequestOptions = options;
            this.eventHubContainer = this.storageClient.GetContainerReference(this.leaseContainerName);
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
            if (lease?.Offset != null)
            {
                checkpoint = new Checkpoint(partitionId)
                {
                    Offset = lease.Offset,
                    SequenceNumber = lease.SequenceNumber
                };
            }

    	    return checkpoint;
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
                return AzureStorageCheckpointLeaseManager.leaseRenewInterval;
            }
        }

        public TimeSpan LeaseDuration
        {
            get
            {
                return AzureStorageCheckpointLeaseManager.leaseDuration;
            }
        }

        public Task<bool> LeaseStoreExistsAsync()
        {
            return this.eventHubContainer.ExistsAsync();
        }

        public Task<bool> CreateLeaseStoreIfNotExistsAsync()
        {
            return this.eventHubContainer.CreateIfNotExistsAsync();
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
                                    ProcessorEventSource.Log.AzureStorageManagerWarning(this.host.Id, "N/A", "Failure while deleting lease store:", e.ToString());
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
                            ProcessorEventSource.Log.AzureStorageManagerWarning(this.host.Id, "N/A", "Failure while deleting lease store:", e.ToString());
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

            if (await leaseBlob.ExistsAsync().ConfigureAwait(false))
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
                    this.host.Id,
                    partitionId,
                    "CreateLeaseIfNotExist - leaseContainerName: " + this.leaseContainerName + 
                    " consumerGroupName: " + this.host.ConsumerGroupName + " storageBlobPrefix: " + this.storageBlobPrefix);
                await leaseBlob.UploadTextAsync(jsonLease, null, AccessCondition.GenerateIfNoneMatchCondition("*"), null, null).ConfigureAwait(false);
            }
    	    catch (StorageException se)
    	    {
    		    StorageExtendedErrorInformation extendedErrorInfo = se.RequestInformation.ExtendedErrorInformation;
    		    if (extendedErrorInfo != null &&
        		    (extendedErrorInfo.ErrorCode == BlobErrorCodeStrings.BlobAlreadyExists ||
    	    	     extendedErrorInfo.ErrorCode == BlobErrorCodeStrings.LeaseIdMissing)) // occurs when somebody else already has leased the blob
    		    {
                    // The blob already exists.
                    ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.Id, partitionId, "Lease already exists");
                    returnLease = (AzureBlobLease)await GetLeaseAsync(partitionId).ConfigureAwait(false);
                }
    		    else
    		    {
                    ProcessorEventSource.Log.AzureStorageManagerError(
                        this.host.Id,
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
            ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.Id, azureBlobLease.PartitionId, "Deleting lease");
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
                await leaseBlob.FetchAttributesAsync().ConfigureAwait(false);
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

                    ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.Id, lease.PartitionId, "Need to ChangeLease");
                    newToken = await leaseBlob.ChangeLeaseAsync(newLeaseId, AccessCondition.GenerateLeaseCondition(lease.Token)).ConfigureAwait(false);
                }
                else
                {
                    ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.Id, lease.PartitionId, "Need to AcquireLease");
                    newToken = await leaseBlob.AcquireLeaseAsync(leaseDuration, newLeaseId).ConfigureAwait(false);
                }

                lease.Token = newToken;
                lease.Owner = this.host.HostName;
                lease.IncrementEpoch(); // Increment epoch each time lease is acquired or stolen by a new host
                await leaseBlob.UploadTextAsync(JsonConvert.SerializeObject(lease), null, AccessCondition.GenerateLeaseCondition(lease.Token), null, null).ConfigureAwait(false);
            }
    	    catch (StorageException se)
            {
                if (WasLeaseLost(partitionId, se))
                {
                    retval = false;
                }
                else
                {
                    throw;
                }
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
                await leaseBlob.RenewLeaseAsync(AccessCondition.GenerateLeaseCondition(lease.Token), this.renewRequestOptions, null).ConfigureAwait(false);
            }
    	    catch (StorageException se)
            {
                if (WasLeaseLost(partitionId, se))
                {
                    throw new LeaseLostException(partitionId, se);
                }

                throw;
            }
    	
 	        return true;
        }

        public Task<bool> ReleaseLeaseAsync(Lease lease)
        {
            return ReleaseLeaseCoreAsync((AzureBlobLease)lease);
        }

        async Task<bool> ReleaseLeaseCoreAsync(AzureBlobLease lease)
        {
            ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.Id, lease.PartitionId, "Releasing lease");

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
                await leaseBlob.UploadTextAsync(JsonConvert.SerializeObject(releasedCopy), null, AccessCondition.GenerateLeaseCondition(leaseId), null, null).ConfigureAwait(false);
                await leaseBlob.ReleaseLeaseAsync(AccessCondition.GenerateLeaseCondition(leaseId)).ConfigureAwait(false);
            }
    	    catch (StorageException se)
            {
                if (WasLeaseLost(partitionId, se))
                {
                    throw new LeaseLostException(partitionId, se);
                }

                throw;
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
            ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.Id, partitionId, "Updating lease");

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
                ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.Id, lease.PartitionId, $"Raw JSON uploading: {jsonToUpload}");
                await leaseBlob.UploadTextAsync(jsonToUpload, null, AccessCondition.GenerateLeaseCondition(token), null, null).ConfigureAwait(false);
            }
    	    catch (StorageException se)
	        {
	            if (WasLeaseLost(partitionId, se))
                {
                    throw new LeaseLostException(partitionId, se);
                }

                throw;
	        }
    	
        	return true;
        }

        async Task<AzureBlobLease> DownloadLeaseAsync(string partitionId, CloudBlockBlob blob) // throws StorageException, IOException
        {
            string jsonLease = await blob.DownloadTextAsync().ConfigureAwait(false);

            ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.Id, partitionId, "Raw JSON downloaded: " + jsonLease);
            AzureBlobLease rehydrated = (AzureBlobLease)JsonConvert.DeserializeObject(jsonLease, typeof(AzureBlobLease));
    	    AzureBlobLease blobLease = new AzureBlobLease(rehydrated, blob);
    	    return blobLease;
        }
    
        bool WasLeaseLost(string partitionId, StorageException se)
        {
            bool retval = false;
            ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.Id, partitionId, "WAS LEASE LOST?");
            ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.Id, partitionId, "HttpStatusCode " + se.RequestInformation.HttpStatusCode);
            if (se.RequestInformation.HttpStatusCode == 409 || // conflict
                se.RequestInformation.HttpStatusCode == 412) // precondition failed
            {
                StorageExtendedErrorInformation extendedErrorInfo = se.RequestInformation.ExtendedErrorInformation;
                if (extendedErrorInfo != null)
                {
                    string errorCode = extendedErrorInfo.ErrorCode;
                    ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.Id, partitionId, "Error code: " + errorCode);
                    ProcessorEventSource.Log.AzureStorageManagerInfo(this.host.Id, partitionId, "Error message: " + extendedErrorInfo.ErrorMessage);
                    if (errorCode == BlobErrorCodeStrings.LeaseLost ||
                        errorCode == BlobErrorCodeStrings.LeaseIdMismatchWithLeaseOperation ||
                        errorCode == BlobErrorCodeStrings.LeaseIdMismatchWithBlobOperation)
                    {
                        retval = true;
                    }
                }
            }

            return retval;
        }

        CloudBlockBlob GetBlockBlobReference(string partitionId)
        {
            CloudBlockBlob leaseBlob = this.consumerGroupDirectory.GetBlockBlobReference(partitionId);

            // GetBlockBlobReference creates a new ServiceClient thus resets options.
            // Because of this we lose settings like MaximumExecutionTime on the client.
            // Until storage addresses the issue we need to override it here once more.
            // Tracking bug: https://github.com/Azure/azure-storage-net/issues/398
            leaseBlob.ServiceClient.DefaultRequestOptions = this.storageClient.DefaultRequestOptions;

            return leaseBlob;
        }
    }
}