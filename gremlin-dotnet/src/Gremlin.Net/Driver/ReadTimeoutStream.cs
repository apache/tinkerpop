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
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Wraps a stream and applies an idle-read timeout to each individual read by linking a
    ///     <see cref="CancellationTokenSource.CancelAfter(TimeSpan)"/> with the caller's token.
    ///     The timeout resets per chunk, so it is an idle-read timeout rather than a whole-request
    ///     deadline. A non-positive timeout disables the behavior.
    /// </summary>
    internal sealed class ReadTimeoutStream : Stream
    {
        private readonly Stream _inner;
        private readonly TimeSpan _readTimeout;

        public ReadTimeoutStream(Stream inner, TimeSpan readTimeout)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _readTimeout = readTimeout;
        }

        private bool TimeoutEnabled => _readTimeout > TimeSpan.Zero;

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            if (!TimeoutEnabled)
            {
                return await _inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            }

            using var timeoutCts = new CancellationTokenSource();
            using var linkedCts =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
            timeoutCts.CancelAfter(_readTimeout);
            try
            {
                return await _inner.ReadAsync(buffer, linkedCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested &&
                                                     !cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException(
                    $"Read timed out after {_readTimeout.TotalSeconds:0.###}s waiting for response data.");
            }
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count,
            CancellationToken cancellationToken)
        {
            return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            // The driver always reads asynchronously; provide a correct sync fallback.
            return _inner.Read(buffer, offset, count);
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() => _inner.Flush();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inner.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
