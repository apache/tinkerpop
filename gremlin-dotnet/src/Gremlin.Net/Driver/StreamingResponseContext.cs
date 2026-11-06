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
using System.IO.Compression;
using System.Net.Http;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Holds the HTTP response resources that must remain alive during streaming
    ///     deserialization. Disposed when the stream is fully consumed, faulted, or cancelled.
    /// </summary>
    internal sealed class StreamingResponseContext : IDisposable
    {
        private readonly HttpResponseMessage _response;
        private readonly Stream _contentStream;
        private readonly Stream? _decompressionStream;

        /// <summary>
        ///     Gets the stream to read from — the decompression stream if present,
        ///     otherwise the raw content stream.
        /// </summary>
        public Stream Stream => _decompressionStream ?? _contentStream;

        /// <summary>
        ///     Initializes a new instance of the <see cref="StreamingResponseContext"/> class.
        /// </summary>
        /// <param name="response">The HTTP response message.</param>
        /// <param name="contentStream">The raw content stream from the response.</param>
        /// <param name="decompressionStream">An optional decompression stream wrapping the content stream.</param>
        public StreamingResponseContext(HttpResponseMessage response, Stream contentStream,
            Stream? decompressionStream = null)
        {
            _response = response;
            _contentStream = contentStream;
            _decompressionStream = decompressionStream;
        }

        /// <summary>
        ///     Disposes the decompression stream (if any), the content stream, and the HTTP response.
        /// </summary>
        public void Dispose()
        {
            _decompressionStream?.Dispose();
            _contentStream.Dispose();
            _response.Dispose();
        }
    }
}
