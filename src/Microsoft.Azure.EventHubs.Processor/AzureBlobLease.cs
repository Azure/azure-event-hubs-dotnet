﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.EventHubs.Processor
{
    using System.Threading.Tasks;
	using Newtonsoft.Json;
	using WindowsAzure.Storage.Blob;

	class AzureBlobLease : Lease
	{
        readonly bool isOwned;

	    // ctor needed for deserialization
	    internal AzureBlobLease()
		{
		}

	    internal AzureBlobLease(string partitionId, CloudBlockBlob blob) : base(partitionId)
		{
			this.Blob = blob;
		}

        internal AzureBlobLease(string partitionId, string owner, bool isOwned, CloudBlockBlob blob) : base(partitionId)
        {
            this.Blob = blob;
            this.Owner = owner;
            this.isOwned = isOwned;
        }

        internal AzureBlobLease(AzureBlobLease source)
			: base(source)
		{
			this.Offset = source.Offset;
			this.SequenceNumber = source.SequenceNumber;
			this.Blob = source.Blob;
		}

	    internal AzureBlobLease(AzureBlobLease source, CloudBlockBlob blob) : base(source)
		{
			this.Offset = source.Offset;
			this.SequenceNumber = source.SequenceNumber;
			this.Blob = blob;
		}

	    // do not serialize
	    [JsonIgnore]
		public CloudBlockBlob Blob { get; }

	    public override Task<bool> IsExpired()
		{
            return Task.FromResult(!this.isOwned);
		}
	}
}