﻿#region License

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

using System.Collections.Generic;
using Gremlin.Net.Driver.Exceptions;

namespace Gremlin.Net.Driver.Messages
{
    /// <summary>
    ///     Represents status information of a <see cref="ResponseMessage{T}"/>.
    /// </summary>
    public record ResponseStatus
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="ResponseStatus" /> record.
        /// </summary>
        /// <param name="code">The <see cref="ResponseStatusCode" />.</param>
        /// <param name="attributes">The (optional) attributes with protocol-level information.</param>
        /// <param name="message">The (optional) message which is just a human-readable string usually associated with errors.</param>
        public ResponseStatus(ResponseStatusCode code, Dictionary<string, object>? attributes = null,
            string? message = null)
        {
            Code = code;
            Attributes = attributes ?? new Dictionary<string, object>();
            Message = message;
        }
        
        /// <summary>
        ///     Gets the <see cref="ResponseStatusCode"/>.
        /// </summary>
        public ResponseStatusCode Code { get; }

        /// <summary>
        ///     Gets the attributes <see cref="Dictionary{TKey,TValue}"/> with protocol-level information.
        /// </summary>
        public Dictionary<string, object> Attributes { get; }

        /// <summary>
        ///     Gets the message which is just a human-readable string usually associated with errors.
        /// </summary>
        public string? Message { get; }
    }

    internal static class ResponseStatusExtensions
    {
        public static void ThrowIfStatusIndicatesError(this ResponseStatus status)
        {
            if (status.Code.IndicatesError())
                throw new ResponseException(status.Code, status.Attributes, $"{status.Code}: {status.Message}");
        }
    }
}