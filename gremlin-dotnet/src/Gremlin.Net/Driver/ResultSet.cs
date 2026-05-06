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

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     A streaming ResultSet returned from the submission of a Gremlin request.
    ///     Implements <see cref="IAsyncEnumerable{T}"/>; so consumers can iterate results as they
    ///     arrive from the server via <c>await foreach</c>.
    ///     Single-consumer only — calling GetAsyncEnumerator a second time throws
    ///     InvalidOperationException.
    /// </summary>
    /// <typeparam name="T">Type of the result elements</typeparam>
    public sealed class ResultSet<T> : IAsyncEnumerable<T>, IAsyncDisposable, IDisposable
    {
        private readonly ChannelReader<object> _channelReader;
        private readonly CancellationTokenSource _disposeCts;
        private readonly Task _backgroundTask;
        // int rather than bool because Interlocked.CompareExchange does not support bool
        private int _enumerated;
        private int _disposed;

        /// <summary>
        ///     Initializes a new <see cref="ResultSet{T}"/> backed by a channel, a cancellation
        ///     source for disposal, and a background task that writes items into the channel.
        /// </summary>
        internal ResultSet(ChannelReader<object> channelReader,
            CancellationTokenSource disposeCts, Task backgroundTask)
        {
            _channelReader = channelReader;
            _disposeCts = disposeCts;
            _backgroundTask = backgroundTask;
        }

        /// <inheritdoc />
        public IAsyncEnumerator<T> GetAsyncEnumerator(
            CancellationToken cancellationToken = default)
        {
            if (Interlocked.CompareExchange(ref _enumerated, 1, 0) != 0)
            {
                throw new InvalidOperationException(
                    "ResultSet can only be enumerated once. Use ToListAsync() if you need " +
                    "to iterate the results multiple times.");
            }

            return EnumerateChannel(cancellationToken);
        }

        private async IAsyncEnumerator<T> EnumerateChannel(CancellationToken cancellationToken)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, _disposeCts.Token);

            await foreach (var item in _channelReader.ReadAllAsync(linked.Token)
                .ConfigureAwait(false))
            {
                yield return (T)item;
            }
        }

        /// <summary>
        ///     Materializes all results into a List&lt;T&gt;.
        ///     This is a convenience method for consumers that need the complete collection.
        ///     Consumes the stream — cannot be called after GetAsyncEnumerator.
        /// </summary>
        public async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
        {
            var results = new List<T>();
            await foreach (var item in this.WithCancellation(cancellationToken)
                .ConfigureAwait(false))
            {
                results.Add(item);
            }
            return results;
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
            {
                return;
            }

            _disposeCts.Cancel();
            try
            {
                await _backgroundTask.ConfigureAwait(false);
            }
            catch
            {
                // Background task may throw OperationCanceledException or other
                // exceptions — swallow them during disposal.
            }
            _disposeCts.Dispose();
        }

        /// <summary>
        ///     Synchronous disposal fallback. Prefer <see cref="DisposeAsync"/> in async contexts
        ///     to avoid blocking the calling thread.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
            {
                return;
            }

            _disposeCts.Cancel();
            try
            {
                _backgroundTask.GetAwaiter().GetResult();
            }
            catch
            {
                // Background task may throw OperationCanceledException or other
                // exceptions — swallow them during disposal.
            }
            _disposeCts.Dispose();
        }
    }
}
