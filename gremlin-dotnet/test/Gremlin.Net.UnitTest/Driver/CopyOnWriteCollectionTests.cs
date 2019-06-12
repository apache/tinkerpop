#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class CopyOnWriteCollectionTests
    {
        [Fact]
        public void ShouldStartEmpty()
        {
            var collection = new CopyOnWriteCollection<int>();
            
            Assert.Equal(new int[0], collection.Snapshot);
        }
        
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(8)]
        public void AddRangeShouldResultInExpectedCount(int expectedCount)
        {
            var items = Enumerable.Range(0, expectedCount).ToArray();
            var collection = new CopyOnWriteCollection<int>();
            
            collection.AddRange(items);
            
            Assert.Equal(expectedCount, collection.Count);
        }
        
        [Fact]
        public void AddRangeShouldAddNewItemsAfterOldItems()
        {
            var oldItems = Enumerable.Range(0, 5).ToArray();
            var newItems = Enumerable.Range(0, 3).ToArray();
            var collection = new CopyOnWriteCollection<int>();
            collection.AddRange(oldItems);
            
            collection.AddRange(newItems);

            var expectedItems = new int[oldItems.Length + newItems.Length];
            oldItems.CopyTo(expectedItems, 0);
            newItems.CopyTo(expectedItems, oldItems.Length);
            Assert.Equal(expectedItems, collection.Snapshot);
        }

        [Fact]
        public void TryRemoveShouldReturnFalseForUnknownItem()
        {
            const int unknownItem = -1;
            var knownItems = Enumerable.Range(0, 5).ToArray();
            var collection = new CopyOnWriteCollection<int>();
            collection.AddRange(knownItems);
            
            Assert.False(collection.TryRemove(unknownItem));
        }
        
        [Fact]
        public void TryRemoveShouldNotChangeCountForUnknownItem()
        {
            const int unknownItem = -1;
            var knownItems = Enumerable.Range(0, 5).ToArray();
            var collection = new CopyOnWriteCollection<int>();
            collection.AddRange(knownItems);
            
            collection.TryRemove(unknownItem);
            
            Assert.Equal(knownItems.Length, collection.Count);
        }

        [Fact]
        public void TryRemoveShouldReturnTrueForKnownItem()
        {
            var knownItems = Enumerable.Range(0, 5).ToArray();
            var collection = new CopyOnWriteCollection<int>();
            collection.AddRange(knownItems);
            
            Assert.True(collection.TryRemove(knownItems[2]));
        }
        
        [Fact]
        public void TryRemoveShouldRemoveKnownItem()
        {
            var knownItems = Enumerable.Range(0, 5).ToArray();
            var collection = new CopyOnWriteCollection<int>();
            collection.AddRange(knownItems);
            
            collection.TryRemove(knownItems[2]);
            
            Assert.DoesNotContain(knownItems[2], collection.Snapshot);
        }
        
        [Fact]
        public void TryRemoveShouldDecrementCountForKnownItem()
        {
            var knownItems = Enumerable.Range(0, 5).ToArray();
            var collection = new CopyOnWriteCollection<int>();
            collection.AddRange(knownItems);
            
            collection.TryRemove(knownItems[2]);

            Assert.Equal(knownItems.Length - 1, collection.Count);
        }
        
        [Fact]
        public void TryRemoveOfLastItemShouldEmptyTheArray()
        {
            var collection = new CopyOnWriteCollection<int>();
            const int lastItem = 3;
            collection.AddRange(new[] {lastItem});
            
            collection.TryRemove(lastItem);

            Assert.Equal(new int[0], collection.Snapshot);
        }
        
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(8)]
        public void SnapshotShouldReturnAddedItems(int nrItems)
        {
            var items = Enumerable.Range(0, nrItems).ToArray();
            var collection = new CopyOnWriteCollection<int>();
            collection.AddRange(items);
            
            Assert.Equal(items, collection.Snapshot);
        }
        
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(8)]
        public void RemoveAndGetAllShouldReturnAllItems(int nrItems)
        {
            var items = Enumerable.Range(0, nrItems).ToArray();
            var collection = new CopyOnWriteCollection<int>();
            collection.AddRange(items);
            
            Assert.Equal(items, collection.RemoveAndGetAll());
        }
        
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(8)]
        public void RemoveAndGetAllShouldEmptyTheArray(int nrItems)
        {
            var items = Enumerable.Range(0, nrItems).ToArray();
            var collection = new CopyOnWriteCollection<int>();
            collection.AddRange(items);

            collection.RemoveAndGetAll();
            
            Assert.Equal(new int[0], collection.Snapshot);
        }

        [Fact]
        public void AddRangeShouldAllowParallelCalls()
        {
            var collection = new CopyOnWriteCollection<int>();
            const int nrOfParallelOperations = 100;
            var addRangeActions = new Action[nrOfParallelOperations];
            var addedItems = new List<int>(nrOfParallelOperations * 3);
            for (var i = 0; i < nrOfParallelOperations; i++)
            {
                var itemsToAdd = new[] {i, i + 1, i + 2};
                addedItems.AddRange(itemsToAdd);
                addRangeActions[i] = () => collection.AddRange(itemsToAdd);
            }
            
            Parallel.Invoke(addRangeActions);
            
            AssertCollectionContainsExactlyUnordered(collection, addedItems);
        }

        private static void AssertCollectionContainsExactlyUnordered(CopyOnWriteCollection<int> collection, IEnumerable<int> expectedItems)
        {
            foreach (var item in expectedItems)
            {
                Assert.True(collection.TryRemove(item));
            }
            Assert.Equal(0, collection.Count);
        }
        
        [Fact]
        public void TryRemoveShouldAllowParallelCalls()
        {
            const int nrOfParallelOperations = 100;
            var items = Enumerable.Range(0, nrOfParallelOperations).ToArray();
            var collection = new CopyOnWriteCollection<int>();
            collection.AddRange(items);
            var tryRemoveActions = new Action[nrOfParallelOperations];
            for (var i = 0; i < nrOfParallelOperations; i++)
            {
                var item = i;
                tryRemoveActions[i] = () => collection.TryRemove(item);
            }
            
            Parallel.Invoke(tryRemoveActions);
            
            Assert.Equal(0, collection.Count);
        }
    }
}