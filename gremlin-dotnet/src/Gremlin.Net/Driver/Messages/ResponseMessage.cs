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

namespace Gremlin.Net.Driver.Messages
{
    /// <summary>
    ///     The 4.0 response message returned from the server.
    /// </summary>
    /// <typeparam name="T">The type of the result data.</typeparam>
    public record ResponseMessage<T>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="ResponseMessage{T}" /> record.
        /// </summary>
        /// <param name="bulked">Whether the response contains bulked results.</param>
        /// <param name="result">The deserialized result data.</param>
        /// <param name="statusCode">The status code from the GraphBinary response status (after the end-of-stream marker).</param>
        /// <param name="statusMessage">Optional status message from the GraphBinary response status.</param>
        /// <param name="exception">Optional exception string from the GraphBinary response status.</param>
        public ResponseMessage(bool bulked, T result, int statusCode,
            string? statusMessage, string? exception)
        {
            Bulked = bulked;
            Result = result;
            StatusCode = statusCode;
            StatusMessage = statusMessage;
            Exception = exception;
        }

        /// <summary>
        ///     Gets whether the response contains bulked results.
        /// </summary>
        public bool Bulked { get; }

        /// <summary>
        ///     Gets the deserialized result data.
        /// </summary>
        public T Result { get; }

        /// <summary>
        ///     Gets the status code from the GraphBinary response status (200 = success).
        /// </summary>
        public int StatusCode { get; }

        /// <summary>
        ///     Gets the optional status message from the GraphBinary response status.
        /// </summary>
        public string? StatusMessage { get; }

        /// <summary>
        ///     Gets the optional exception string from the GraphBinary response status.
        /// </summary>
        public string? Exception { get; }
    }
}
