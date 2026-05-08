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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class ResultSetTests
    {
        /// <summary>
        ///     Creates a ResultSet backed by a channel. The background task writes
        ///     the provided items to the channel and then completes it.
        /// </summary>
        private static ResultSet<T> CreateResultSet<T>(IEnumerable<T> items)
        {
            var channel = Channel.CreateUnbounded<object>(
                new UnboundedChannelOptions { SingleWriter = true });
            var disposeCts = new CancellationTokenSource();

            var backgroundTask = Task.Run(async () =>
            {
                foreach (var item in items)
                {
                    await channel.Writer.WriteAsync(item!).ConfigureAwait(false);
                }
                channel.Writer.Complete();
            });

            return new ResultSet<T>(channel.Reader, disposeCts, backgroundTask);
        }

        /// <summary>
        ///     Creates a ResultSet whose background task completes the channel with an error.
        /// </summary>
        private static ResultSet<T> CreateFaultedResultSet<T>(Exception exception)
        {
            var channel = Channel.CreateUnbounded<object>(
                new UnboundedChannelOptions { SingleWriter = true });
            var disposeCts = new CancellationTokenSource();

            var backgroundTask = Task.Run(() =>
            {
                channel.Writer.Complete(exception);
            });

            return new ResultSet<T>(channel.Reader, disposeCts, backgroundTask);
        }

        /// <summary>
        ///     Creates a ResultSet whose background task blocks until the dispose CTS is cancelled.
        /// </summary>
        private static (ResultSet<T> resultSet, TaskCompletionSource<bool> started) CreateBlockingResultSet<T>()
        {
            var channel = Channel.CreateUnbounded<object>(
                new UnboundedChannelOptions { SingleWriter = true });
            var disposeCts = new CancellationTokenSource();
            var started = new TaskCompletionSource<bool>();

            var backgroundTask = Task.Run(async () =>
            {
                started.SetResult(true);
                try
                {
                    // Block until cancelled
                    await Task.Delay(Timeout.Infinite, disposeCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Expected on dispose
                }
                finally
                {
                    channel.Writer.Complete();
                }
            });

            return (new ResultSet<T>(channel.Reader, disposeCts, backgroundTask), started);
        }

        [Fact]
        public async Task AwaitForeachShouldYieldAllResults()
        {
            var expected = new List<int> { 1, 2, 3, 4, 5 };
            await using var resultSet = CreateResultSet(expected);

            var actual = new List<int>();
            await foreach (var item in resultSet)
            {
                actual.Add(item);
            }

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task AwaitForeachShouldYieldStringResults()
        {
            var expected = new List<string> { "hello", "world", "gremlin" };
            await using var resultSet = CreateResultSet(expected);

            var actual = new List<string>();
            await foreach (var item in resultSet)
            {
                actual.Add(item);
            }

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task AwaitForeachShouldHandleEmptyResultSet()
        {
            await using var resultSet = CreateResultSet(new List<int>());

            var actual = new List<int>();
            await foreach (var item in resultSet)
            {
                actual.Add(item);
            }

            Assert.Empty(actual);
        }

        [Fact]
        public async Task ToListAsyncShouldReturnAllResultsInOrder()
        {
            var expected = new List<int> { 10, 20, 30, 40, 50 };
            await using var resultSet = CreateResultSet(expected);

            var actual = await resultSet.ToListAsync();

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task ToListAsyncShouldReturnEmptyListForEmptyResultSet()
        {
            await using var resultSet = CreateResultSet(new List<string>());

            var actual = await resultSet.ToListAsync();

            Assert.Empty(actual);
        }

        [Fact]
        public async Task DisposeAsyncShouldCancelBackgroundTask()
        {
            var (resultSet, started) = CreateBlockingResultSet<int>();
            await started.Task;

            var disposeTask = resultSet.DisposeAsync().AsTask();
            var completed = await Task.WhenAny(disposeTask, Task.Delay(5000));

            Assert.Same(disposeTask, completed);
        }

        [Fact]
        public async Task ErrorInChannelShouldPropagateToConsumerDuringIteration()
        {
            var expectedException = new InvalidOperationException("deserialization failed");
            await using var resultSet = CreateFaultedResultSet<int>(expectedException);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (var _ in resultSet)
                {
                    // Should not yield any items
                }
            });

            Assert.Same(expectedException, ex);
        }

        [Fact]
        public async Task ErrorInChannelShouldPropagateToToListAsync()
        {
            var expectedException = new InvalidOperationException("stream error");
            await using var resultSet = CreateFaultedResultSet<string>(expectedException);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => resultSet.ToListAsync());

            Assert.Same(expectedException, ex);
        }

        [Fact]
        public async Task SecondCallToGetAsyncEnumeratorShouldThrowInvalidOperationException()
        {
            await using var resultSet = CreateResultSet(new List<int> { 1, 2, 3 });

            // First call should succeed
            var enumerator = resultSet.GetAsyncEnumerator();
            // Consume to avoid leaving the enumerator in a bad state
            while (await enumerator.MoveNextAsync())
            {
            }

            // Second call throws immediately — the guard check is eager (not deferred
            // to MoveNextAsync) so callers get fast feedback.
            Assert.Throws<InvalidOperationException>(
                () => resultSet.GetAsyncEnumerator());
        }

        [Fact]
        public async Task ToListAsyncAfterGetAsyncEnumeratorShouldThrowInvalidOperationException()
        {
            await using var resultSet = CreateResultSet(new List<int> { 1 });

            // Consume via await foreach first
            await foreach (var _ in resultSet)
            {
            }

            // ToListAsync calls GetAsyncEnumerator internally, so it should throw
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => resultSet.ToListAsync());
        }

        [Fact]
        public void DisposeShouldNotThrowWhenCalledSynchronously()
        {
            var resultSet = CreateResultSet(new List<int> { 1, 2, 3 });

            // Synchronous Dispose should not throw
            resultSet.Dispose();
        }

        [Fact]
        public async Task DoubleDisposeAsyncShouldNotThrow()
        {
            var resultSet = CreateResultSet(new List<int> { 1, 2, 3 });

            await resultSet.DisposeAsync();
            await resultSet.DisposeAsync();
            // If we get here without throwing, double-dispose is safe
        }

        [Fact]
        public void DoubleDisposeSyncShouldNotThrow()
        {
            var resultSet = CreateResultSet(new List<int> { 1, 2, 3 });

            resultSet.Dispose();
            resultSet.Dispose();
            // If we get here without throwing, double sync dispose is safe
        }

        [Fact]
        public async Task MixedDisposeAsyncThenSyncShouldNotThrow()
        {
            var resultSet = CreateResultSet(new List<int> { 1, 2, 3 });

            await resultSet.DisposeAsync();
            resultSet.Dispose();
            // Mixed disposal order should not throw
        }

        [Fact]
        public async Task CancellationDuringIterationShouldStopEnumeration()
        {
            // Create a channel that won't complete on its own — items trickle in slowly
            var channel = Channel.CreateUnbounded<object>(
                new UnboundedChannelOptions { SingleWriter = true });
            var disposeCts = new CancellationTokenSource();
            var backgroundTask = Task.Run(async () =>
            {
                for (var i = 0; i < 100; i++)
                {
                    await channel.Writer.WriteAsync(i).ConfigureAwait(false);
                    try
                    {
                        await Task.Delay(50, disposeCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
                channel.Writer.Complete();
            });

            var resultSet = new ResultSet<int>(channel.Reader, disposeCts, backgroundTask);
            using var iterationCts = new CancellationTokenSource();

            var collected = new List<int>();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await foreach (var item in resultSet.WithCancellation(iterationCts.Token))
                {
                    collected.Add(item);
                    if (collected.Count >= 2)
                    {
                        iterationCts.Cancel();
                    }
                }
            });

            Assert.True(collected.Count >= 2, "Should have collected at least 2 items before cancellation");
            await resultSet.DisposeAsync();
        }
    }
}
