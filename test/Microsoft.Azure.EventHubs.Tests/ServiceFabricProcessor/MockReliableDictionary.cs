using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Data.Notifications;

namespace Microsoft.Azure.EventHubs.Tests.ServiceFabricProcessor
{
    class MockReliableDictionary<X, Y> : IReliableDictionary<X, Y> where X : System.IEquatable<X>, System.IComparable<X>
    {
        private Dictionary<X, Y> innerDictionary = new Dictionary<X, Y>();

        public Task SetAsync(ITransaction tx, X key, Y value, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Puts key/value in dictionary, throwing away any current value for key.
            this.innerDictionary[key] = value;
            return Task.CompletedTask;
        }

        public Task<ConditionalValue<Y>> TryGetValueAsync(ITransaction tx, X key, TimeSpan timeout, CancellationToken cancellationToken)
        {
            if (this.innerDictionary.ContainsKey(key))
            {
                return Task.FromResult<ConditionalValue<Y>>(new ConditionalValue<Y>(true, this.innerDictionary[key]));
            }
            Y dummy = default(Y);
            return Task.FromResult<ConditionalValue<Y>>(new ConditionalValue<Y>(false, dummy));
        }

        #region unused
        // Unused
        public Func<IReliableDictionary<X, Y>, NotifyDictionaryRebuildEventArgs<X, Y>, Task> RebuildNotificationAsyncCallback { set => throw new NotImplementedException(); }

        // Unused
        public Uri Name => throw new NotImplementedException();

        // Unused
        public event EventHandler<NotifyDictionaryChangedEventArgs<X, Y>> DictionaryChanged;

        public Task AddAsync(ITransaction tx, X key, Y value)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task AddAsync(ITransaction tx, X key, Y value, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<Y> AddOrUpdateAsync(ITransaction tx, X key, Func<X, Y> addValueFactory, Func<X, Y, Y> updateValueFactory)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<Y> AddOrUpdateAsync(ITransaction tx, X key, Y addValue, Func<X, Y, Y> updateValueFactory)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<Y> AddOrUpdateAsync(ITransaction tx, X key, Func<X, Y> addValueFactory, Func<X, Y, Y> updateValueFactory, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<Y> AddOrUpdateAsync(ITransaction tx, X key, Y addValue, Func<X, Y, Y> updateValueFactory, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task ClearAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task ClearAsync()
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<bool> ContainsKeyAsync(ITransaction tx, X key)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<bool> ContainsKeyAsync(ITransaction tx, X key, LockMode lockMode)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<bool> ContainsKeyAsync(ITransaction tx, X key, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<bool> ContainsKeyAsync(ITransaction tx, X key, LockMode lockMode, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerable<KeyValuePair<X, Y>>> CreateEnumerableAsync(ITransaction txn)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerable<KeyValuePair<X, Y>>> CreateEnumerableAsync(ITransaction txn, EnumerationMode enumerationMode)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerable<KeyValuePair<X, Y>>> CreateEnumerableAsync(ITransaction txn, Func<X, bool> filter, EnumerationMode enumerationMode)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<long> GetCountAsync(ITransaction tx)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<Y> GetOrAddAsync(ITransaction tx, X key, Func<X, Y> valueFactory)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<Y> GetOrAddAsync(ITransaction tx, X key, Y value)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<Y> GetOrAddAsync(ITransaction tx, X key, Func<X, Y> valueFactory, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<Y> GetOrAddAsync(ITransaction tx, X key, Y value, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task SetAsync(ITransaction tx, X key, Y value)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<bool> TryAddAsync(ITransaction tx, X key, Y value)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<bool> TryAddAsync(ITransaction tx, X key, Y value, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<ConditionalValue<Y>> TryGetValueAsync(ITransaction tx, X key)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<ConditionalValue<Y>> TryGetValueAsync(ITransaction tx, X key, LockMode lockMode)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<ConditionalValue<Y>> TryGetValueAsync(ITransaction tx, X key, LockMode lockMode, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<ConditionalValue<Y>> TryRemoveAsync(ITransaction tx, X key)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<ConditionalValue<Y>> TryRemoveAsync(ITransaction tx, X key, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<bool> TryUpdateAsync(ITransaction tx, X key, Y newValue, Y comparisonValue)
        {
            // Unused
            throw new NotImplementedException();
        }

        public Task<bool> TryUpdateAsync(ITransaction tx, X key, Y newValue, Y comparisonValue, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Unused
            throw new NotImplementedException();
        }
        #endregion
    }
}
