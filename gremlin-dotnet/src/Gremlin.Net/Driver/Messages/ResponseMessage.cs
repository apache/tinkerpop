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

namespace Gremlin.Net.Driver.Messages
{
    /// <summary>
    ///     The message returned from the server.
    /// </summary>
    /// <typeparam name="T">The type of the data returned.</typeparam>
    public record ResponseMessage<T>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="ResponseMessage{T}" /> record.
        /// </summary>
        /// <param name="requestId">The identifier of the <see cref="RequestMessage"/> that generated this <see cref="ResponseMessage{T}"/>.</param>
        /// <param name="status">Status information about this <see cref="ResponseMessage{T}"/>.</param>
        /// <param name="result">The result with its data and optional meta information.</param>
        public ResponseMessage(Guid? requestId, ResponseStatus status, ResponseResult<T> result)
        {
            RequestId = requestId;
            Status = status;
            Result = result;
        }
        
        /// <summary>
        ///     Gets the identifier of the <see cref="RequestMessage"/> that generated this <see cref="ResponseMessage{T}"/>.
        /// </summary>
        public Guid? RequestId { get; }

        /// <summary>
        ///     Gets status information about this <see cref="ResponseMessage{T}"/>.
        /// </summary>
        public ResponseStatus Status { get; }

        /// <summary>
        ///     Gets the result with its data and optional meta information.
        /// </summary>
        public ResponseResult<T> Result { get; set; }
    }
}