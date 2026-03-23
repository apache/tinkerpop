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
using System.Security.Cryptography;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Mutable HTTP request context passed to request interceptors.
    /// </summary>
    public class HttpRequestContext
    {
        /// <summary>
        ///     Gets or sets the HTTP method (e.g. "POST").
        /// </summary>
        public string Method { get; set; }

        /// <summary>
        ///     Gets or sets the request URI.
        /// </summary>
        public Uri Uri { get; set; }

        /// <summary>
        ///     Gets the HTTP headers. Interceptors may add, modify, or remove entries.
        /// </summary>
        public Dictionary<string, string> Headers { get; }

        /// <summary>
        ///     Gets or sets the request body. This is <c>byte[]</c> when serialization has occurred
        ///     (default path), or <c>RequestMessage</c> when serialization is deferred to interceptors
        ///     (<c>requestSerializer = null</c>). Interceptors may also set this to an
        ///     <see cref="System.Net.Http.HttpContent"/> instance for full control over the wire format.
        /// </summary>
        public object Body { get; set; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="HttpRequestContext" /> class.
        /// </summary>
        /// <param name="method">The HTTP method.</param>
        /// <param name="uri">The request URI.</param>
        /// <param name="headers">The HTTP headers.</param>
        /// <param name="body">The request body. Typically <c>byte[]</c> (post-serialization) or
        ///     <c>RequestMessage</c> (pre-serialization).</param>
        public HttpRequestContext(string method, Uri uri, Dictionary<string, string> headers, object body)
        {
            Method = method ?? throw new ArgumentNullException(nameof(method));
            Uri = uri ?? throw new ArgumentNullException(nameof(uri));
            Headers = headers ?? throw new ArgumentNullException(nameof(headers));
            Body = body;
        }

        /// <summary>
        ///     Returns the lowercase hex-encoded SHA-256 digest of the body.
        ///     Throws <see cref="InvalidOperationException"/> if <see cref="Body"/> is not <c>byte[]</c>,
        ///     which indicates that serialization has not yet occurred.
        /// </summary>
        public string GetPayloadHash()
        {
            if (Body is not byte[] bytes)
            {
                throw new InvalidOperationException(
                    "Cannot compute payload hash before serialization. " +
                    "Body must be byte[] but is " +
                    (Body?.GetType().Name ?? "null") + ".");
            }
            using var sha256 = SHA256.Create();
            var hash = sha256.ComputeHash(bytes);
            return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
        }
    }
}
