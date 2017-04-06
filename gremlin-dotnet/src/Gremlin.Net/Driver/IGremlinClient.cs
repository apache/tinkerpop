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
using Gremlin.Net.Driver.Messages;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Provides a mechanism for submitting Gremlin requests.
    /// </summary>
    public interface IGremlinClient : IDisposable
    {
        /// <summary>
        ///     Submits a request message as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T">The type of the expected results.</typeparam>
        /// <param name="requestMessage">The <see cref="RequestMessage" /> to send.</param>
        /// <returns>A collection of the data returned from the server.</returns>
        /// <exception cref="Exceptions.ResponseException">
        ///     Thrown when a response is received from Gremlin Server that indicates
        ///     that an error occurred.
        /// </exception>
        Task<IReadOnlyCollection<T>> SubmitAsync<T>(RequestMessage requestMessage);
    }
}