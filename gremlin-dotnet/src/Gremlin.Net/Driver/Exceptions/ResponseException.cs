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

namespace Gremlin.Net.Driver.Exceptions
{
    /// <summary>
    ///     The exception that is thrown when a response is received from Gremlin Server that indicates that an error occurred.
    /// </summary>
    public class ResponseException : Exception
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="ResponseException" /> class.
        /// </summary>
        /// <param name="statusCode">The status code from the GraphBinary status footer.</param>
        /// <param name="statusMessage">The status message from the server.</param>
        /// <param name="serverException">The exception class name from the server, if provided.</param>
        public ResponseException(int statusCode, string? statusMessage, string? serverException)
            : base(statusMessage ?? $"Server error: {statusCode}")
        {
            StatusCode = statusCode;
            ServerException = serverException;
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="ResponseException" /> class for a response that
        ///     could not be deserialized. There is no status code from the server in this case since the
        ///     failure occurred locally while reading the response.
        /// </summary>
        /// <param name="innerException">The exception that caused the deserialization failure.</param>
        public ResponseException(Exception innerException)
            : base("Failed to deserialize the response received from Gremlin Server.", innerException)
        {
            StatusCode = NoStatusCode;
        }

        /// <summary>
        ///     The <see cref="StatusCode" /> value used when the exception was not raised from a status
        ///     code reported by the server (e.g. a local deserialization failure).
        /// </summary>
        public const int NoStatusCode = -1;

        /// <summary>
        ///     Gets the status code from the GraphBinary status footer, or <see cref="NoStatusCode" /> if this
        ///     exception represents a local deserialization failure rather than a server-reported error.
        /// </summary>
        public int StatusCode { get; }

        /// <summary>
        ///     Gets the exception class name from the server, if provided.
        /// </summary>
        public string? ServerException { get; }
    }
}
