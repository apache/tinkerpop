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
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class AsyncAutoResetEventTests
    {
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMilliseconds(100);
        
        [Fact]
        public async Task WaitOneAsync_AfterSet_CompletesSynchronously()
        {
            var are = new AsyncAutoResetEvent();

            are.Set();
            var task = are.WaitOneAsync(DefaultTimeout);
            
            Assert.True(task.IsCompleted);
            Assert.True(await task);
        }
        
        [Fact]
        public async Task MultipleWaitOneAsync_AfterSet_OnlyFirstWaitIsSuccessful()
        {
            var are = new AsyncAutoResetEvent();

            are.Set();
            var task1 = are.WaitOneAsync(DefaultTimeout);
            var task2 = are.WaitOneAsync(DefaultTimeout);

            Assert.True(task1.IsCompleted);
            Assert.True(await task1);
            Assert.False(await task2);
        }

        [Fact]
        public async Task MultipleWaitOneAsync_AfterMultipleSet_OnlyFirstWaitIsSuccessful()
        {
            var are = new AsyncAutoResetEvent();

            are.Set();
            are.Set();
            var task1 = are.WaitOneAsync(DefaultTimeout);
            var task2 = are.WaitOneAsync(DefaultTimeout);

            Assert.True(task1.IsCompleted);
            Assert.True(await task1);
            Assert.False(await task2);
        }
        
        [Fact]
        public async Task WaitOneAsync_SetBeforeTimeout_WaitSuccessful()
        {
            var are = new AsyncAutoResetEvent();
            
            var task = are.WaitOneAsync(DefaultTimeout);
            are.Set();
            
            Assert.True(await task);
        }

        [Fact]
        public async Task Set_AfterMultipleWaitOneAsync_OnlyFirstWaitIsSuccessful()
        {
            var are = new AsyncAutoResetEvent();
            
            var task1 = are.WaitOneAsync(DefaultTimeout);
            var task2 = are.WaitOneAsync(DefaultTimeout);
            are.Set();

            await AssertCompletesBeforeTimeoutAsync(task1, DefaultTimeout.Milliseconds + 50);
            Assert.False(await task2);
        }
        
        [Fact]
        public async Task WaitOneAsync_NotSet_OnlyWaitUntilTimeout()
        {
            var are = new AsyncAutoResetEvent();

            var task = are.WaitOneAsync(DefaultTimeout);

            await AssertCompletesBeforeTimeoutAsync(task, DefaultTimeout.Milliseconds + 50);
        }
        
        [Fact]
        public async Task WaitOneAsync_NotSet_WaitNotSuccessful()
        {
            var are = new AsyncAutoResetEvent();

            var task = are.WaitOneAsync(DefaultTimeout);

            Assert.False(await task);
        }

        [Fact]
        public async Task WaitOneAsync_SetAfterPreviousWaitTimedOut_OnlySecondWaitSuccessful()
        {
            var are = new AsyncAutoResetEvent();

            var task1 = are.WaitOneAsync(DefaultTimeout);
            await Task.Delay(DefaultTimeout + TimeSpan.FromMilliseconds(50));
            var task2 = are.WaitOneAsync(DefaultTimeout);
            are.Set();
            
            Assert.False(await task1);
            Assert.True(await task2);
        }
        
        [Fact]
        public async Task WaitOneAsync_SetAfterMultipleWaitsTimedOut_OnlyLastWaitSuccessful()
        {
            var are = new AsyncAutoResetEvent();

            var timedOutTasks = new List<Task<bool>>();
            for (var i = 0; i < 1000; i++)
            {
                timedOutTasks.Add(are.WaitOneAsync(DefaultTimeout));
            }
            
            await Task.Delay(DefaultTimeout + TimeSpan.FromMilliseconds(50));
            var task2 = are.WaitOneAsync(DefaultTimeout);
            are.Set();

            foreach (var t in timedOutTasks)
            {
                Assert.False(await t);
            }
            Assert.True(await task2);
        }

        private static async Task AssertCompletesBeforeTimeoutAsync(Task task, int timeoutInMs)
        {
            var completedTask = await WaitForTaskOrTimeoutAsync(task, TimeSpan.FromMilliseconds(timeoutInMs));
            if (completedTask != task)
                throw new Exception("Task did not complete.");
        }

        private static Task<Task> WaitForTaskOrTimeoutAsync(Task task, TimeSpan timeout)
        {
            return Task.WhenAny(task, Task.Delay(timeout));
        }
    }
}