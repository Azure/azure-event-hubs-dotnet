// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.using System;

using System;
using System.Collections.Generic;

namespace Microsoft.Azure.EventHubs.ServiceFabricProcessor
{
    public class Checkpoint
    {
        public Checkpoint(int version)
        {
            this.Version = version;
            this.Valid = false;
        }

        public Checkpoint(string offset, long sequenceNumber)
        {
            this.Version = 1;
            this.Offset = offset;
            this.SequenceNumber = sequenceNumber;
            this.Valid = true;
        }

        //
        // Methods and properties valid for all versions.
        //

        public int Version { get; protected set; }

        public bool Valid { get; protected set; }

        public Dictionary<string, object> ToDictionary()
        {
            Dictionary<string, object> converted = new Dictionary<string, object>();

            converted.Add(Constants.CheckpointPropertyVersion, this.Version);
            converted.Add(Constants.CheckpointPropertyValid, this.Valid);

            switch (this.Version)
            {
                case 1:
                    converted.Add(Constants.CheckpointPropertyOffsetV1, this.Offset);
                    converted.Add(Constants.CheckpointPropertySequenceNumberV1, this.SequenceNumber);
                    break;

                default:
                    throw new NotImplementedException();
            }

            return converted;
        }

        static public Checkpoint CreateFromDictionary(Dictionary<string, object> dictionary)
        {
            int version = (int)dictionary[Constants.CheckpointPropertyVersion];
            bool valid = (bool)dictionary[Constants.CheckpointPropertyValid];

            Checkpoint result = new Checkpoint(version);

            if (valid)
            {
                result.Valid = true;

                switch (result.Version)
                {
                    case 1:
                        result.Offset = (string)dictionary[Constants.CheckpointPropertyOffsetV1];
                        result.SequenceNumber = (long)dictionary[Constants.CheckpointPropertySequenceNumberV1];
                        break;

                    default:
                        throw new NotImplementedException("Unrecognized checkpoint version " + result.Version);
                }
            }

            return result;
        }

        //
        // Methods and properties for Version==1
        //

        public void InitializeV1(string offset, long sequenceNumber)
        {
            this.Version = 1;

            if (string.IsNullOrEmpty(offset))
            {
                throw new ArgumentException("offset must not be null or empty");
            }
            if (sequenceNumber < 0)
            {
                throw new ArgumentException("sequenceNumber must be >= 0");
            }

            this.Offset = offset;
            this.SequenceNumber = sequenceNumber;

            this.Valid = true;
        }

        public string Offset { get; private set; }

        public long SequenceNumber { get; private set; }
    }
}
